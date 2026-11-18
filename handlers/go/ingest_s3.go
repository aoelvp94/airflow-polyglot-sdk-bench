package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/apache/airflow/go-sdk/airflow"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/jackc/pgx/v5"
)

// S3BatchConfig is the payload plan_s3_batch returns. Decoded strictly, like
// BatchConfig: it must list exactly the keys Python sends.
type S3BatchConfig struct {
	BatchID     string `json:"batch_id"`
	SourceTable string `json:"source_table"`
	ConnID      string `json:"conn_id"`
	S3ConnID    string `json:"s3_conn_id"`
	Bucket      string `json:"bucket"`
	Key         string `json:"key"`
}

type IngestS3Args struct {
	Cfg S3BatchConfig `arg:"cfg"`
}

const (
	partSize  = 8 << 20 // 8 MiB multipart parts, same in every language
	gzipLevel = 6
)

// IngestS3 streams `COPY src TO STDOUT` through the shared per-row step, gzips
// it, and uploads it to S3 as one object in sequential 8 MiB multipart parts.
func IngestS3(actx airflow.Context, args IngestS3Args) (map[string]any, error) {
	cfg := args.Cfg
	m := startMetrics()
	ctx := context.Context(actx)

	client, err := s3Client(actx, cfg.S3ConnID)
	if err != nil {
		return nil, err
	}
	src, err := pgConnect(actx, cfg.ConnID)
	if err != nil {
		return nil, err
	}
	defer src.Close(ctx)

	// extract: COPY OUT -> pipe; transform + gzip + upload run in this goroutine.
	extractR, extractW := io.Pipe()
	srcIdent := pgx.Identifier(strings.Split(cfg.SourceTable, ".")).Sanitize()
	extractErr := make(chan error, 1)
	go func() {
		_, err := src.PgConn().CopyTo(ctx, extractW, "COPY "+srcIdent+" TO STDOUT")
		extractW.CloseWithError(err)
		extractErr <- err
	}()

	up, err := newPartUploader(ctx, client, cfg.Bucket, cfg.Key)
	if err != nil {
		return nil, err
	}
	gz, _ := gzip.NewWriterLevel(up, gzipLevel)
	var rows, bytesIn int64
	err = transform(extractR, gz, cfg.BatchID, &rows, &bytesIn)
	if err == nil {
		err = gz.Close()
	}
	if err == nil {
		err = up.complete()
	}
	if err != nil {
		extractR.CloseWithError(err)
		up.abort()
		return nil, fmt.Errorf("ingest to s3: %w", err)
	}
	if err := <-extractErr; err != nil {
		return nil, fmt.Errorf("copy out: %w", err)
	}

	out := m.finish(rows, bytesIn)
	out["batch_id"] = cfg.BatchID
	out["language"] = "go"
	out["source_table"] = cfg.SourceTable
	out["target"] = "s3://" + cfg.Bucket + "/" + cfg.Key
	out["row_count"] = rows
	out["bytes_out"] = up.bytesOut
	out["parts"] = len(up.parts)
	actx.Logger().InfoContext(actx, "ingest to s3 finished", "rows", rows, "bytes_out", up.bytesOut)
	return out, nil
}

// partUploader is an io.Writer that uploads every full 8 MiB of what it is
// given as one multipart part, sequentially.
type partUploader struct {
	ctx      context.Context
	client   *s3.Client
	bucket   string
	key      string
	uploadID *string
	buf      bytes.Buffer
	parts    []types.CompletedPart
	bytesOut int64
}

func newPartUploader(ctx context.Context, c *s3.Client, bucket, key string) (*partUploader, error) {
	res, err := c.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: &bucket, Key: &key, ContentType: aws.String("application/gzip"),
	})
	if err != nil {
		return nil, fmt.Errorf("create multipart upload: %w", err)
	}
	u := &partUploader{ctx: ctx, client: c, bucket: bucket, key: key, uploadID: res.UploadId}
	u.buf.Grow(partSize + 64<<10)
	return u, nil
}

func (u *partUploader) Write(p []byte) (int, error) {
	n, _ := u.buf.Write(p)
	for u.buf.Len() >= partSize {
		if err := u.uploadPart(u.buf.Next(partSize)); err != nil {
			return 0, err
		}
	}
	return n, nil
}

func (u *partUploader) uploadPart(data []byte) error {
	num := int32(len(u.parts) + 1)
	res, err := u.client.UploadPart(u.ctx, &s3.UploadPartInput{
		Bucket: &u.bucket, Key: &u.key, UploadId: u.uploadID, PartNumber: &num,
		Body: bytes.NewReader(data), ContentLength: aws.Int64(int64(len(data))),
	})
	if err != nil {
		return fmt.Errorf("upload part %d: %w", num, err)
	}
	u.parts = append(u.parts, types.CompletedPart{ETag: res.ETag, PartNumber: &num})
	u.bytesOut += int64(len(data))
	return nil
}

func (u *partUploader) complete() error {
	if u.buf.Len() > 0 || len(u.parts) == 0 {
		if err := u.uploadPart(u.buf.Bytes()); err != nil {
			return err
		}
		u.buf.Reset()
	}
	_, err := u.client.CompleteMultipartUpload(u.ctx, &s3.CompleteMultipartUploadInput{
		Bucket: &u.bucket, Key: &u.key, UploadId: u.uploadID,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: u.parts},
	})
	if err != nil {
		return fmt.Errorf("complete multipart upload: %w", err)
	}
	return nil
}

func (u *partUploader) abort() {
	_, _ = u.client.AbortMultipartUpload(context.Background(), &s3.AbortMultipartUploadInput{
		Bucket: &u.bucket, Key: &u.key, UploadId: u.uploadID,
	})
}

// s3Client builds a client from an Airflow "aws" connection:
// login/password = access key/secret, extra.endpoint_url / extra.region_name.
func s3Client(actx airflow.Context, connID string) (*s3.Client, error) {
	conn, err := actx.Client().GetConnection(actx, connID)
	if err != nil {
		return nil, fmt.Errorf("get connection %q: %w", connID, err)
	}
	endpoint, _ := conn.Extra["endpoint_url"].(string)
	region, _ := conn.Extra["region_name"].(string)
	if region == "" {
		region = "us-east-1"
	}
	var ak, sk string
	if conn.Login != nil {
		ak = *conn.Login
	}
	if conn.Password != nil {
		sk = *conn.Password
	}
	opts := s3.Options{
		Region:                     region,
		Credentials:                credentials.NewStaticCredentialsProvider(ak, sk, ""),
		UsePathStyle:               true,
		RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
		ResponseChecksumValidation: aws.ResponseChecksumValidationWhenRequired,
	}
	if endpoint != "" {
		opts.BaseEndpoint = aws.String(endpoint)
	}
	return s3.New(opts), nil
}
