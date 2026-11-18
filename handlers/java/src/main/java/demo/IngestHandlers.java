package demo;

import org.apache.airflow.sdk.Bundle;
import org.apache.airflow.sdk.Server;

/**
 * Java TaskHandlers for the Python Dags {@code orders_elt_pipeline} and
 * {@code ingest_bench}. The Python files declare the graph with
 * {@code @task.stub(queue="java")}; JavaCoordinator starts one JVM per task
 * instance with every JAR of the {@code java-task-handlers} bundle on the
 * classpath and runs this Main-Class.
 */
public final class IngestHandlers {
  private IngestHandlers() {}

  public static void main(String[] args) {
    Bundle bundle = new Bundle()
        .register("orders_elt_pipeline", "ingest_java", IngestTask.class)
        .register("ingest_bench", "ingest_java", IngestTask.class)
        .register("ingest_bench", "noop_java", NoopTask.class)
        .register("ingest_bench_s3", "ingest_s3_java", IngestS3Task.class)
        .register("ingest_bench_s3", "noop_java", NoopTask.class);
    Server.create(args).serve(bundle);
  }
}
