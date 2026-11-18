package demo;

import java.util.Map;
import org.apache.airflow.sdk.Client;
import org.apache.airflow.sdk.Context;
import org.apache.airflow.sdk.Task;

/** Measures pure task startup: coordinator spawn + JVM start + SDK handshake. */
public class NoopTask implements Task {
  @Override
  public void execute(Context context, Client client) {
    Map<String, Object> result = Metrics.start().finish(0, 0);
    result.put("language", "java");
    client.setXCom("return_value", result);
  }
}
