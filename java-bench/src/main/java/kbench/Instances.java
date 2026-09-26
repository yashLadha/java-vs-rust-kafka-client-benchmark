package kbench;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import org.HdrHistogram.Histogram;

final class Instances {
    private Instances() {
    }

    static void validate(int instances, int partitions) {
        if (instances < 1) {
            throw new IllegalArgumentException("--instances must be >= 1");
        }
        if (partitions < instances) {
            throw new IllegalArgumentException("--partitions (" + partitions + ") must be >= --instances (" + instances + ")");
        }
    }

    static long share(long total, int instances, int k) {
        return total / instances + (k < total % instances ? 1 : 0);
    }

    static int[] owned(int partitions, int instances, int k) {
        int[] out = new int[(partitions - k + instances - 1) / instances];
        for (int j = 0; j < out.length; j++) {
            out[j] = k + j * instances;
        }
        return out;
    }

    static void await(CyclicBarrier barrier) {
        try {
            barrier.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted at start barrier", e);
        } catch (BrokenBarrierException e) {
            throw new IllegalStateException("start barrier broken", e);
        }
    }

    // Instance 0 runs on the calling thread so K=1 has the same thread count as the single-instance harness.
    static void runAll(List<? extends Runnable> instances, String threadPrefix) throws InterruptedException {
        List<Thread> threads = new ArrayList<>();
        for (int k = 1; k < instances.size(); k++) {
            Thread t = new Thread(instances.get(k), threadPrefix + k);
            t.start();
            threads.add(t);
        }
        instances.get(0).run();
        for (Thread t : threads) {
            t.join();
        }
    }

    static Map<String, Object> detail(int k, long messages, long errors, long durationNs, Histogram hist) {
        Map<String, Object> d = new LinkedHashMap<>();
        double s = durationNs / 1e9;
        d.put("k", k);
        d.put("messages", messages);
        d.put("errors", errors);
        d.put("duration_s", s);
        d.put("throughput_msgs_per_s", s > 0 ? messages / s : 0.0);
        if (hist == null) {
            d.put("latency_us", null);
        } else {
            Map<String, Object> l = new LinkedHashMap<>();
            long n = hist.getTotalCount();
            l.put("p50", hist.getValueAtPercentile(50.0));
            l.put("p99", hist.getValueAtPercentile(99.0));
            l.put("max", n > 0 ? hist.getMaxValue() : 0);
            d.put("latency_us", l);
        }
        return d;
    }
}
