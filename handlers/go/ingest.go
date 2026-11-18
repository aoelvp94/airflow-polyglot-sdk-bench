package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/apache/airflow/go-sdk/airflow"
	"github.com/jackc/pgx/v5"
)

// BatchConfig is the payload plan_batch returns. The SDK decodes it with
// DisallowUnknownFields, so this struct must list exactly the keys Python
// sends: adding a key on the Python side without adding it here fails the task.
type BatchConfig struct {
	BatchID     string `json:"batch_id"`
	SourceTable string `json:"source_table"`
	TargetTable string `json:"target_table"`
	ConnID      string `json:"conn_id"`
}

// IngestArgs binds the stub's single `cfg` argument. A sole struct's fields
// bind by `arg:` tag, so the whole dict lands in Cfg.
type IngestArgs struct {
	Cfg BatchConfig `arg:"cfg"`
}

const fieldsPerRow = 8 // columns in src.orders_template

// Ingest streams `COPY src TO STDOUT` through a per-row step into
// `COPY target FROM STDIN`, inside one transaction that first truncates the
// target. The per-row step is identical in every language: check the field
// count, append "\t<batch_id>", count the row.
func Ingest(actx airflow.Context, args IngestArgs) (map[string]any, error) {
	cfg := args.Cfg
	m := startMetrics()
	logger := actx.Logger()

	ctx := context.Context(actx)
	src, err := pgConnect(actx, cfg.ConnID)
	if err != nil {
		return nil, fmt.Errorf("connect (source): %w", err)
	}
	defer src.Close(ctx)
	dst, err := pgConnect(actx, cfg.ConnID)
	if err != nil {
		return nil, fmt.Errorf("connect (target): %w", err)
	}
	defer dst.Close(ctx)

	tx, err := dst.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	if _, err := tx.Exec(ctx, "TRUNCATE "+pgx.Identifier(strings.Split(cfg.TargetTable, ".")).Sanitize()); err != nil {
		return nil, fmt.Errorf("truncate: %w", err)
	}

	// extract: COPY OUT -> pipe A; transform: pipe A -> pipe B; load: pipe B -> COPY IN.
	extractR, extractW := io.Pipe()
	loadR, loadW := io.Pipe()
	srcIdent := pgx.Identifier(strings.Split(cfg.SourceTable, ".")).Sanitize()
	dstIdent := pgx.Identifier(strings.Split(cfg.TargetTable, ".")).Sanitize()

	extractErr := make(chan error, 1)
	go func() {
		_, err := src.PgConn().CopyTo(ctx, extractW, "COPY "+srcIdent+" TO STDOUT")
		extractW.CloseWithError(err)
		extractErr <- err
	}()

	var rows, bytesIn int64
	transformErr := make(chan error, 1)
	go func() {
		err := transform(extractR, loadW, cfg.BatchID, &rows, &bytesIn)
		loadW.CloseWithError(err)
		extractR.CloseWithError(err)
		transformErr <- err
	}()

	if _, err := tx.Conn().PgConn().CopyFrom(ctx, loadR, "COPY "+dstIdent+" FROM STDIN"); err != nil {
		return nil, fmt.Errorf("copy in: %w", err)
	}
	if err := <-transformErr; err != nil {
		return nil, fmt.Errorf("transform: %w", err)
	}
	if err := <-extractErr; err != nil {
		return nil, fmt.Errorf("copy out: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit: %w", err)
	}

	out := m.finish(rows, bytesIn)
	out["batch_id"] = cfg.BatchID
	out["language"] = "go"
	out["source_table"] = cfg.SourceTable
	out["target_table"] = cfg.TargetTable
	out["row_count"] = rows
	logger.InfoContext(actx, "ingest finished", "rows", rows, "elapsed_s", out["handler_elapsed_s"])
	return out, nil
}

// pgConnect opens a connection from an Airflow "postgres" connection.
func pgConnect(actx airflow.Context, connID string) (*pgx.Conn, error) {
	conn, err := actx.Client().GetConnection(actx, connID)
	if err != nil {
		return nil, fmt.Errorf("get connection %q: %w", connID, err)
	}
	pgcfg, err := pgx.ParseConfig("")
	if err != nil {
		return nil, err
	}
	pgcfg.Host, pgcfg.Port, pgcfg.Database = conn.Host, uint16(conn.Port), conn.Path
	if conn.Login != nil {
		pgcfg.User = *conn.Login
	}
	if conn.Password != nil {
		pgcfg.Password = *conn.Password
	}
	return pgx.ConnectConfig(actx, pgcfg)
}

func transform(r io.Reader, w io.Writer, batchID string, rows, bytesIn *int64) error {
	in := bufio.NewReaderSize(r, 1<<20)
	out := bufio.NewWriterSize(w, 1<<20)
	suffix := []byte("\t" + batchID + "\n")
	for {
		line, err := in.ReadSlice('\n')
		if err == bufio.ErrBufferFull {
			return fmt.Errorf("row longer than 1 MiB")
		}
		if len(line) > 0 {
			*bytesIn += int64(len(line))
			if line[len(line)-1] != '\n' {
				return fmt.Errorf("truncated row at end of stream")
			}
			if n := bytes.Count(line, []byte{'\t'}) + 1; n != fieldsPerRow {
				return fmt.Errorf("row %d has %d fields, want %d", *rows+1, n, fieldsPerRow)
			}
			if _, err := out.Write(line[:len(line)-1]); err != nil {
				return err
			}
			if _, err := out.Write(suffix); err != nil {
				return err
			}
			*rows++
		}
		if err == io.EOF {
			return out.Flush()
		}
		if err != nil {
			return err
		}
	}
}

// Noop measures pure task startup: coordinator spawn + process init + SDK handshake.
func Noop(actx airflow.Context) (map[string]any, error) {
	out := startMetrics().finish(0, 0)
	out["language"] = "go"
	return out, nil
}

// --- metrics, read the same way in every language from /proc/self ---------

type metrics struct {
	wallStart time.Time
	cpuStart  float64
	preTask   float64
}

const clkTck = 100.0 // USER_HZ on Linux

func startMetrics() metrics {
	return metrics{wallStart: time.Now(), cpuStart: procCPU(), preTask: processAge()}
}

func (m metrics) finish(rows, bytesIn int64) map[string]any {
	elapsed := time.Since(m.wallStart).Seconds()
	cpuEnd := procCPU()
	out := map[string]any{
		"handler_elapsed_s":      round3(elapsed),
		"process_age_at_start_s": round3(m.preTask),
		"cpu_handler_s":          round3(cpuEnd - m.cpuStart),
		"cpu_process_total_s":    round3(cpuEnd),
		"peak_rss_mb":            round3(vmHWMKB() / 1024),
		"bytes_in":               bytesIn,
	}
	if elapsed > 0 && rows > 0 {
		out["rows_per_s"] = int64(float64(rows) / elapsed)
	}
	return out
}

func round3(f float64) float64 { return float64(int64(f*1000+0.5)) / 1000 }

func statFields() []string {
	b, err := os.ReadFile("/proc/self/stat")
	if err != nil {
		return nil
	}
	s := string(b)
	// comm (field 2) may contain spaces; fields after the closing paren are fixed.
	return strings.Fields(s[strings.LastIndexByte(s, ')')+2:])
}

func procCPU() float64 {
	f := statFields()
	if len(f) < 13 {
		return 0
	}
	ut, _ := strconv.ParseFloat(f[11], 64) // utime (field 14)
	st, _ := strconv.ParseFloat(f[12], 64) // stime (field 15)
	return (ut + st) / clkTck
}

func processAge() float64 {
	f := statFields()
	if len(f) < 20 {
		return 0
	}
	startTicks, _ := strconv.ParseFloat(f[19], 64) // starttime (field 22)
	b, err := os.ReadFile("/proc/uptime")
	if err != nil {
		return 0
	}
	up, _ := strconv.ParseFloat(strings.Fields(string(b))[0], 64)
	return up - startTicks/clkTck
}

func vmHWMKB() float64 {
	b, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(line, "VmHWM:") {
			v, _ := strconv.ParseFloat(strings.Fields(line)[1], 64)
			return v
		}
	}
	return 0
}
