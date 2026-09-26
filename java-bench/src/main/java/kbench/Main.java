package kbench;

import java.io.PrintStream;
import java.util.LinkedHashMap;
import java.util.Map;
import org.HdrHistogram.Histogram;

public final class Main {
    static final String CLIENT_VERSION = "4.3.1";

    private Main() {
    }

    public static void main(String[] argv) {
        long mainStartNs = System.nanoTime();
        // Anything that writes to System.out (including third party code) is diverted to stderr so
        // stdout carries exactly one RESULT line.
        PrintStream stdout = System.out;
        System.setOut(System.err);

        String mode = argv.length > 0 ? argv[0] : "";
        Map<String, Object> r;
        int exit;
        switch (mode) {
            case "produce", "consume" -> {
                r = baseResult(mode);
                try {
                    if (mode.equals("produce")) {
                        ProduceMode.run(argv, mainStartNs, r);
                    } else {
                        ConsumeMode.run(argv, mainStartNs, r);
                    }
                } catch (Throwable t) {
                    Log.error("run failed", t);
                    r.put("status", "error");
                    r.put("error", t.toString());
                }
                exit = switch ((String) r.get("status")) {
                    case "ok" -> 0;
                    case "timeout" -> 3;
                    default -> 1;
                };
            }
            case "payload-hash" -> {
                r = new LinkedHashMap<>();
                exit = 0;
                try {
                    PayloadHash.run(argv, r);
                } catch (Throwable t) {
                    Log.error("payload-hash failed", t);
                    r.put("status", "error");
                    r.put("error", t.toString());
                    exit = 1;
                }
            }
            default -> {
                System.err.println("usage: kbench-java produce|consume|payload-hash [flags]");
                r = baseResult(mode.isEmpty() ? null : mode);
                r.put("status", "error");
                r.put("error", "unknown mode: " + mode);
                exit = 2;
            }
        }
        stdout.println("RESULT " + Json.write(r));
        stdout.flush();
        System.err.flush();
        System.exit(exit);
    }

    static Map<String, Object> baseResult(String mode) {
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("schema_version", 1);
        r.put("status", "ok");
        r.put("error", null);
        r.put("client", "java");
        r.put("client_lib", "kafka-clients");
        r.put("client_version", CLIENT_VERSION);
        r.put("native_lib_version", null);
        r.put("runtime", runtime());
        r.put("mode", mode);
        r.put("run_id", null);
        r.put("instances", null);
        r.put("params", null);
        r.put("effective_config", null);
        r.put("payload_sha256", null);
        r.put("pool_count", null);
        r.put("messages", 0L);
        r.put("bytes", 0L);
        r.put("errors", 0L);
        r.put("truncated", false);
        r.put("window_kind", null);
        r.put("duration_s", 0.0);
        r.put("throughput_msgs_per_s", 0.0);
        r.put("throughput_mb_per_s", 0.0);
        r.put("rate_lag_max_us", null);
        r.put("latency_kind", null);
        r.put("latency_us", null);
        r.put("startup_ms", null);
        r.put("first_message_ms", null);
        r.put("resources", null);
        r.put("jvm", null);
        r.put("instances_detail", null);
        r.put("timeseries", java.util.List.of());
        return r;
    }

    private static String runtime() {
        String vendorVersion = System.getProperty("java.vendor.version");
        String base = vendorVersion != null ? vendorVersion.replace('-', ' ') : "Java " + System.getProperty("java.runtime.version");
        return base + " (" + System.getProperty("java.runtime.version") + ", " + System.getProperty("java.vm.name") + ")";
    }

    static Histogram newHistogram() {
        return new Histogram(120_000_000L, 3);
    }

    static Map<String, Object> latencyJson(Histogram h) {
        Map<String, Object> l = new LinkedHashMap<>();
        long n = h.getTotalCount();
        l.put("count", n);
        l.put("min", n > 0 ? h.getMinValue() : 0);
        l.put("mean", n > 0 ? h.getMean() : 0.0);
        l.put("stddev", n > 0 ? h.getStdDeviation() : 0.0);
        l.put("p50", h.getValueAtPercentile(50.0));
        l.put("p90", h.getValueAtPercentile(90.0));
        l.put("p99", h.getValueAtPercentile(99.0));
        l.put("p99_9", h.getValueAtPercentile(99.9));
        l.put("p99_99", h.getValueAtPercentile(99.99));
        l.put("max", n > 0 ? h.getMaxValue() : 0);
        return l;
    }

    static void fillThroughput(Map<String, Object> r, long messages, long bytes, long durationNs) {
        double s = durationNs / 1e9;
        r.put("messages", messages);
        r.put("bytes", bytes);
        r.put("duration_s", s);
        r.put("throughput_msgs_per_s", s > 0 ? messages / s : 0.0);
        r.put("throughput_mb_per_s", s > 0 ? bytes / s / 1e6 : 0.0);
    }

    static double ms(long ns) {
        return ns / 1e6;
    }

    static long epochNanos() {
        java.time.Instant now = java.time.Instant.now();
        return now.getEpochSecond() * 1_000_000_000L + now.getNano();
    }
}
