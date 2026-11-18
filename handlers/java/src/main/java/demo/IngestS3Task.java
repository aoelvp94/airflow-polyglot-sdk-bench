package demo;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.zip.Deflater;
import java.util.zip.GZIPOutputStream;
import org.apache.airflow.sdk.Client;
import org.apache.airflow.sdk.Context;
import org.apache.airflow.sdk.InputTask;
import org.apache.airflow.sdk.TaskInput;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyOut;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.checksums.RequestChecksumCalculation;
import software.amazon.awssdk.core.checksums.ResponseChecksumValidation;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;

/**
 * Streams {@code COPY src TO STDOUT} through the shared per-row step, gzips it
 * (level 6) and uploads one object in sequential 8 MiB multipart parts.
 */
public class IngestS3Task implements InputTask<IngestS3Task.Input> {
  private static final int FIELDS_PER_ROW = 8;
  private static final int PART_SIZE = 8 << 20;
  private static final int GZIP_LEVEL = 6;
  private static final ObjectMapper JSON = new ObjectMapper();

  public static class Input implements TaskInput {
    public S3BatchConfig cfg;
  }

  /** Exactly the keys plan_s3_batch returns (the SDK's mapper rejects unknown ones). */
  public static class S3BatchConfig {
    @JsonProperty("batch_id") public String batchId;
    @JsonProperty("source_table") public String sourceTable;
    @JsonProperty("conn_id") public String connId;
    @JsonProperty("s3_conn_id") public String s3ConnId;
    @JsonProperty("bucket") public String bucket;
    @JsonProperty("key") public String key;
  }

  @Override
  public void execute(Context context, Client client, Input input) throws Exception {
    S3BatchConfig cfg = input.cfg;
    Metrics m = Metrics.start();

    org.apache.airflow.sdk.Connection pg = client.getConnection(cfg.connId);
    Properties props = new Properties();
    props.setProperty("user", pg.login);
    props.setProperty("password", pg.password);
    String url = "jdbc:postgresql://" + pg.host + ":" + pg.port + "/" + pg.schema;

    long rows = 0;
    long bytesIn = 0;
    try (S3Client s3 = s3Client(client.getConnection(cfg.s3ConnId));
         Connection src = DriverManager.getConnection(url, props)) {
      PartUploader up = new PartUploader(s3, cfg.bucket, cfg.key);
      try {
        CopyOut out = src.unwrap(PGConnection.class).getCopyAPI()
            .copyOut("COPY " + IngestTask.quoteIdent(cfg.sourceTable) + " TO STDOUT");
        byte[] suffix = ("\t" + cfg.batchId + "\n").getBytes(StandardCharsets.UTF_8);
        try (GZIPOutputStream gz = new Level6Gzip(up)) {
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
            gz.write(row, 0, len - 1);
            gz.write(suffix);
            rows++;
          }
        }
        up.complete();
      } catch (Exception e) {
        up.abort();
        throw e;
      }

      Map<String, Object> result = m.finish(rows, bytesIn);
      result.put("batch_id", cfg.batchId);
      result.put("language", "java");
      result.put("source_table", cfg.sourceTable);
      result.put("target", "s3://" + cfg.bucket + "/" + cfg.key);
      result.put("row_count", rows);
      result.put("bytes_out", up.bytesOut);
      result.put("parts", up.parts.size());
      client.setXCom("return_value", result);
    }
  }

  /** GZIPOutputStream with an explicit level (the JDK default is also 6) and a 64 KiB buffer. */
  private static final class Level6Gzip extends GZIPOutputStream {
    Level6Gzip(OutputStream out) throws IOException {
      super(out, 64 << 10);
      def.setLevel(GZIP_LEVEL);
    }
  }

  /** Buffers compressed bytes and uploads each full 8 MiB as one part, sequentially. */
  private static final class PartUploader extends OutputStream {
    private final S3Client s3;
    private final String bucket;
    private final String key;
    private final String uploadId;
    private final ByteArrayOutputStream buf = new ByteArrayOutputStream(PART_SIZE + (64 << 10));
    final List<CompletedPart> parts = new ArrayList<>();
    long bytesOut = 0;
    private boolean closed = false;

    PartUploader(S3Client s3, String bucket, String key) {
      this.s3 = s3;
      this.bucket = bucket;
      this.key = key;
      this.uploadId = s3.createMultipartUpload(
          b -> b.bucket(bucket).key(key).contentType("application/gzip")).uploadId();
    }

    @Override
    public void write(int b) {
      buf.write(b);
      maybeUpload();
    }

    @Override
    public void write(byte[] b, int off, int len) {
      buf.write(b, off, len);
      maybeUpload();
    }

    private void maybeUpload() {
      if (buf.size() < PART_SIZE) return;
      byte[] all = buf.toByteArray();
      buf.reset();
      int off = 0;
      while (all.length - off >= PART_SIZE) {
        uploadPart(all, off, PART_SIZE);
        off += PART_SIZE;
      }
      buf.write(all, off, all.length - off);
    }

    private void uploadPart(byte[] data, int off, int len) {
      int num = parts.size() + 1;
      String etag = s3.uploadPart(
          b -> b.bucket(bucket).key(key).uploadId(uploadId).partNumber(num).contentLength((long) len),
          RequestBody.fromByteBuffer(java.nio.ByteBuffer.wrap(data, off, len))).eTag();
      parts.add(CompletedPart.builder().partNumber(num).eTag(etag).build());
      bytesOut += len;
    }

    /** GZIPOutputStream.close() closes us too; completion is explicit, so this only flags it. */
    @Override
    public void close() {
      closed = true;
    }

    void complete() {
      if (!closed) throw new IllegalStateException("gzip stream not closed");
      if (buf.size() > 0 || parts.isEmpty()) {
        byte[] rest = buf.toByteArray();
        buf.reset();
        uploadPart(rest, 0, rest.length);
      }
      s3.completeMultipartUpload(b -> b.bucket(bucket).key(key).uploadId(uploadId)
          .multipartUpload(CompletedMultipartUpload.builder().parts(parts).build()));
    }

    void abort() {
      try {
        s3.abortMultipartUpload(b -> b.bucket(bucket).key(key).uploadId(uploadId));
      } catch (RuntimeException ignored) {
        // best effort
      }
    }
  }

  /**
   * Builds a client from an Airflow "aws" connection. Connection.extra arrives
   * from the Java SDK as a JSON string, not a map, so it is parsed here.
   */
  static S3Client s3Client(org.apache.airflow.sdk.Connection conn) throws IOException {
    JsonNode extra = conn.extra == null ? JSON.createObjectNode()
        : conn.extra instanceof String s ? JSON.readTree(s) : JSON.valueToTree(conn.extra);
    String region = extra.path("region_name").asText("us-east-1");
    var b = S3Client.builder()
        .region(Region.of(region))
        .credentialsProvider(StaticCredentialsProvider.create(
            AwsBasicCredentials.create(conn.login, conn.password)))
        .forcePathStyle(true)
        .requestChecksumCalculation(RequestChecksumCalculation.WHEN_REQUIRED)
        .responseChecksumValidation(ResponseChecksumValidation.WHEN_REQUIRED)
        .httpClient(UrlConnectionHttpClient.create());
    if (extra.hasNonNull("endpoint_url")) b.endpointOverride(URI.create(extra.get("endpoint_url").asText()));
    return b.build();
  }
}
