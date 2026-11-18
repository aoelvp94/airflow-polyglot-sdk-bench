package demo;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;
import org.apache.airflow.sdk.Client;
import org.apache.airflow.sdk.Context;
import org.apache.airflow.sdk.InputTask;
import org.apache.airflow.sdk.TaskInput;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyIn;
import org.postgresql.copy.CopyManager;
import org.postgresql.copy.CopyOut;

/**
 * Streams {@code COPY src TO STDOUT} through the shared per-row step into
 * {@code COPY target FROM STDIN}, inside one transaction that first truncates
 * the target. Single-threaded: CopyOut and CopyIn are pull/push chunk APIs, so
 * no pipe or extra thread is needed.
 */
public class IngestTask implements InputTask<IngestTask.Input> {
  private static final int FIELDS_PER_ROW = 8;

  /** The stub's single {@code cfg} argument; TaskInput fields bind by name. */
  public static class Input implements TaskInput {
    public BatchConfig cfg;
  }

  /**
   * Payload of plan_batch. The SDK converts it with a default Jackson mapper,
   * which fails on unknown properties, so this must list exactly the keys
   * Python sends. Names are matched exactly, hence @JsonProperty.
   */
  public static class BatchConfig {
    @JsonProperty("batch_id") public String batchId;
    @JsonProperty("source_table") public String sourceTable;
    @JsonProperty("target_table") public String targetTable;
    @JsonProperty("conn_id") public String connId;
  }

  @Override
  public void execute(Context context, Client client, Input input) throws Exception {
    BatchConfig cfg = input.cfg;
    Metrics m = Metrics.start();

    org.apache.airflow.sdk.Connection conn = client.getConnection(cfg.connId);
    String url = "jdbc:postgresql://" + conn.host + ":" + conn.port + "/" + conn.schema;
    Properties props = new Properties();
    props.setProperty("user", conn.login);
    props.setProperty("password", conn.password);

    long rows = 0;
    long bytesIn = 0;
    try (Connection src = DriverManager.getConnection(url, props);
         Connection dst = DriverManager.getConnection(url, props)) {
      dst.setAutoCommit(false);
      try (Statement st = dst.createStatement()) {
        st.execute("TRUNCATE " + quoteIdent(cfg.targetTable));
      }
      CopyManager srcCopy = src.unwrap(PGConnection.class).getCopyAPI();
      CopyManager dstCopy = dst.unwrap(PGConnection.class).getCopyAPI();

      CopyOut out = srcCopy.copyOut("COPY " + quoteIdent(cfg.sourceTable) + " TO STDOUT");
      CopyIn in = dstCopy.copyIn("COPY " + quoteIdent(cfg.targetTable) + " FROM STDIN");
      byte[] suffix = ("\t" + cfg.batchId + "\n").getBytes(StandardCharsets.UTF_8);
      byte[] buf = new byte[1 << 20];
      int bufLen = 0;
      try {
        // The driver returns one CopyData message per row in text mode.
        byte[] row;
        while ((row = out.readFromCopy()) != null) {
          bytesIn += row.length;
          int len = row.length;
          if (len == 0 || row[len - 1] != '\n') {
            throw new IllegalStateException("truncated row at end of stream");
          }
          int tabs = 0;
          for (int i = 0; i < len; i++) {
            if (row[i] == '\t') tabs++;
          }
          if (tabs + 1 != FIELDS_PER_ROW) {
            throw new IllegalStateException(
                "row " + (rows + 1) + " has " + (tabs + 1) + " fields, want " + FIELDS_PER_ROW);
          }
          int need = len - 1 + suffix.length;
          if (bufLen + need > buf.length) {
            in.writeToCopy(buf, 0, bufLen);
            bufLen = 0;
          }
          System.arraycopy(row, 0, buf, bufLen, len - 1);
          bufLen += len - 1;
          System.arraycopy(suffix, 0, buf, bufLen, suffix.length);
          bufLen += suffix.length;
          rows++;
        }
        if (bufLen > 0) in.writeToCopy(buf, 0, bufLen);
        in.endCopy();
      } catch (Exception e) {
        if (in.isActive()) in.cancelCopy();
        if (out.isActive()) out.cancelCopy();
        throw e;
      }
      dst.commit();
    }

    Map<String, Object> result = m.finish(rows, bytesIn);
    result.put("batch_id", cfg.batchId);
    result.put("language", "java");
    result.put("source_table", cfg.sourceTable);
    result.put("target_table", cfg.targetTable);
    result.put("row_count", rows);
    client.setXCom("return_value", result);
  }

  /** Quotes schema.table as "schema"."table". */
  static String quoteIdent(String dotted) {
    StringBuilder sb = new StringBuilder();
    for (String part : dotted.split("\\.")) {
      if (sb.length() > 0) sb.append('.');
      sb.append('"').append(part.replace("\"", "\"\"")).append('"');
    }
    return sb.toString();
  }
}
