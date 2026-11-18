package demo;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

/** Reads process metrics from /proc/self, the same way every language does. */
final class Metrics {
  private static final double CLK_TCK = 100.0; // USER_HZ on Linux

  private final long wallStartNanos;
  private final double cpuStart;
  private final double processAgeAtStart;

  private Metrics(long wallStartNanos, double cpuStart, double processAgeAtStart) {
    this.wallStartNanos = wallStartNanos;
    this.cpuStart = cpuStart;
    this.processAgeAtStart = processAgeAtStart;
  }

  static Metrics start() {
    return new Metrics(System.nanoTime(), procCpu(), processAge());
  }

  Map<String, Object> finish(long rows, long bytesIn) {
    double elapsed = (System.nanoTime() - wallStartNanos) / 1e9;
    double cpuEnd = procCpu();
    Map<String, Object> out = new LinkedHashMap<>();
    out.put("handler_elapsed_s", round3(elapsed));
    out.put("process_age_at_start_s", round3(processAgeAtStart));
    out.put("cpu_handler_s", round3(cpuEnd - cpuStart));
    out.put("cpu_process_total_s", round3(cpuEnd));
    out.put("peak_rss_mb", round3(vmHwmKb() / 1024.0));
    out.put("bytes_in", bytesIn);
    if (elapsed > 0 && rows > 0) out.put("rows_per_s", (long) (rows / elapsed));
    return out;
  }

  private static double round3(double v) {
    return Math.round(v * 1000.0) / 1000.0;
  }

  private static String[] statFields() {
    try {
      String s = Files.readString(Path.of("/proc/self/stat"));
      return s.substring(s.lastIndexOf(')') + 2).trim().split("\\s+");
    } catch (IOException e) {
      return new String[0];
    }
  }

  private static double procCpu() {
    String[] f = statFields();
    if (f.length < 13) return 0;
    return (Double.parseDouble(f[11]) + Double.parseDouble(f[12])) / CLK_TCK;
  }

  private static double processAge() {
    String[] f = statFields();
    if (f.length < 20) return 0;
    try {
      double up = Double.parseDouble(Files.readString(Path.of("/proc/uptime")).trim().split("\\s+")[0]);
      return up - Double.parseDouble(f[19]) / CLK_TCK;
    } catch (IOException e) {
      return 0;
    }
  }

  private static double vmHwmKb() {
    try {
      for (String line : Files.readAllLines(Path.of("/proc/self/status"))) {
        if (line.startsWith("VmHWM:")) return Double.parseDouble(line.split("\\s+")[1]);
      }
    } catch (IOException e) {
      // fall through
    }
    return 0;
  }
}
