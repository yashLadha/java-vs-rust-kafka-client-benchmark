package kbench;

import java.io.IOException;
import java.lang.management.CompilationMXBean;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

final class Resources {
    private static final long SAMPLE_INTERVAL_NS = TimeUnit.MILLISECONDS.toNanos(100);
    private static final Path PROC_STATUS = Path.of("/proc/self/status");
    private static final Path PROC_STAT = Path.of("/proc/self/stat");
    private static final Path CG1_CPU = Path.of("/sys/fs/cgroup/cpuacct/cpuacct.usage");
    private static final Path CG1_MEM_PEAK = Path.of("/sys/fs/cgroup/memory/memory.max_usage_in_bytes");
    private static final Path CG2_CPU = Path.of("/sys/fs/cgroup/cpu.stat");
    private static final Path CG2_MEM_PEAK = Path.of("/sys/fs/cgroup/memory.peak");

    final int clkTck;
    final String clkTckSource;

    private final MemoryMXBean memBean = ManagementFactory.getMemoryMXBean();
    private final List<long[]> samples = new ArrayList<>();
    private volatile boolean running;
    private Thread thread;

    private long[] cpuStart;
    private long[] cpuEnd;
    private Long cgCpuStartNs;
    private Long cgCpuEndNs;
    private long startNs;
    private long endNs;

    private List<GarbageCollectorMXBean> gcBeans;
    private long[] gcCountStart;
    private long[] gcTimeStart;
    private long jitStart;

    Resources() {
        int tck = 100;
        String src = "assumed";
        try {
            Process p = new ProcessBuilder("getconf", "CLK_TCK").redirectErrorStream(true).start();
            String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.US_ASCII).trim();
            if (p.waitFor() == 0) {
                tck = Integer.parseInt(out);
                src = "getconf";
            }
        } catch (IOException | NumberFormatException e) {
            Log.warn("getconf CLK_TCK failed, assuming 100: " + e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        clkTck = tck;
        clkTckSource = src;
    }

    void start() {
        gcBeans = ManagementFactory.getGarbageCollectorMXBeans();
        gcCountStart = new long[gcBeans.size()];
        gcTimeStart = new long[gcBeans.size()];
        for (int i = 0; i < gcBeans.size(); i++) {
            gcCountStart[i] = gcBeans.get(i).getCollectionCount();
            gcTimeStart[i] = gcBeans.get(i).getCollectionTime();
        }
        CompilationMXBean comp = ManagementFactory.getCompilationMXBean();
        jitStart = comp != null && comp.isCompilationTimeMonitoringSupported() ? comp.getTotalCompilationTime() : 0;
        cgCpuStartNs = readCgroupCpuNs();
        cpuStart = readProcCpuTicks();
        startNs = System.nanoTime();
        sampleNow();
        running = true;
        thread = new Thread(this::loop, "kbench-sampler");
        thread.setDaemon(true);
        thread.start();
    }

    void stop() {
        if (thread == null || !running) {
            return;
        }
        running = false;
        LockSupport.unpark(thread);
        try {
            thread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        endNs = System.nanoTime();
        cpuEnd = readProcCpuTicks();
        cgCpuEndNs = readCgroupCpuNs();
        sampleNow();
    }

    boolean started() {
        return thread != null;
    }

    private void loop() {
        long next = System.nanoTime() + SAMPLE_INTERVAL_NS;
        while (running) {
            long wait = next - System.nanoTime();
            if (wait > 0) {
                LockSupport.parkNanos(wait);
                continue;
            }
            sampleNow();
            next += SAMPLE_INTERVAL_NS;
        }
    }

    private void sampleNow() {
        long rss = -1;
        long threads = -1;
        try {
            for (String line : Files.readAllLines(PROC_STATUS, StandardCharsets.US_ASCII)) {
                if (line.startsWith("VmRSS:")) {
                    rss = parseKb(line) * 1024L;
                } else if (line.startsWith("Threads:")) {
                    threads = Long.parseLong(line.substring("Threads:".length()).trim());
                }
            }
        } catch (IOException | NumberFormatException e) {
            // Leave -1 so non-Linux runs still produce a result.
        }
        long heap = memBean.getHeapMemoryUsage().getUsed();
        long[] s = {System.nanoTime(), rss, threads, heap};
        synchronized (samples) {
            samples.add(s);
        }
    }

    private static long parseKb(String line) {
        String[] parts = line.trim().split("\\s+");
        return Long.parseLong(parts[1]);
    }

    private static long[] readProcCpuTicks() {
        try {
            String stat = Files.readString(PROC_STAT, StandardCharsets.US_ASCII);
            // comm (field 2) may contain spaces, so split after the last ')'.
            String[] f = stat.substring(stat.lastIndexOf(')') + 2).trim().split("\\s+");
            return new long[] {Long.parseLong(f[11]), Long.parseLong(f[12])};
        } catch (IOException | RuntimeException e) {
            return null;
        }
    }

    private static Long readCgroupCpuNs() {
        try {
            if (Files.exists(CG1_CPU)) {
                return Long.parseLong(Files.readString(CG1_CPU).trim());
            }
            if (Files.exists(CG2_CPU)) {
                for (String line : Files.readAllLines(CG2_CPU)) {
                    if (line.startsWith("usage_usec ")) {
                        return Long.parseLong(line.substring("usage_usec ".length()).trim()) * 1000L;
                    }
                }
            }
        } catch (IOException | NumberFormatException e) {
            Log.warn("cgroup cpu read failed: " + e);
        }
        return null;
    }

    private static Long readCgroupMemPeak() {
        try {
            if (Files.exists(CG1_MEM_PEAK)) {
                return Long.parseLong(Files.readString(CG1_MEM_PEAK).trim());
            }
            if (Files.exists(CG2_MEM_PEAK)) {
                return Long.parseLong(Files.readString(CG2_MEM_PEAK).trim());
            }
        } catch (IOException | NumberFormatException e) {
            Log.warn("cgroup memory read failed: " + e);
        }
        return null;
    }

    List<long[]> samples() {
        synchronized (samples) {
            return new ArrayList<>(samples);
        }
    }

    Map<String, Object> resourcesJson(long messages) {
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("clk_tck", clkTck);
        r.put("clk_tck_source", clkTckSource);
        Double user = null;
        Double sys = null;
        Double total = null;
        if (cpuStart != null && cpuEnd != null) {
            user = (cpuEnd[0] - cpuStart[0]) / (double) clkTck;
            sys = (cpuEnd[1] - cpuStart[1]) / (double) clkTck;
            total = user + sys;
        }
        double wallS = (endNs - startNs) / 1e9;
        r.put("cpu_user_s", user);
        r.put("cpu_sys_s", sys);
        r.put("cpu_total_s", total);
        r.put("cpu_cores_avg", total != null && wallS > 0 ? total / wallS : null);
        r.put("cpu_s_per_million_msgs", total != null && messages > 0 ? total / messages * 1e6 : null);
        List<long[]> s = samples();
        long rssStart = -1;
        long rssSum = 0;
        long rssN = 0;
        long rssPeak = -1;
        long threadsPeak = -1;
        for (long[] x : s) {
            if (x[1] >= 0) {
                if (rssStart < 0) {
                    rssStart = x[1];
                }
                rssSum += x[1];
                rssN++;
                rssPeak = Math.max(rssPeak, x[1]);
            }
            threadsPeak = Math.max(threadsPeak, x[2]);
        }
        r.put("rss_start_bytes", rssStart >= 0 ? rssStart : null);
        r.put("rss_avg_bytes", rssN > 0 ? rssSum / rssN : null);
        r.put("rss_peak_bytes", rssPeak >= 0 ? rssPeak : null);
        r.put("threads_peak", threadsPeak >= 0 ? threadsPeak : null);
        r.put("cgroup_cpu_s", cgCpuStartNs != null && cgCpuEndNs != null ? (cgCpuEndNs - cgCpuStartNs) / 1e9 : null);
        r.put("cgroup_mem_peak_bytes", readCgroupMemPeak());
        return r;
    }

    Map<String, Object> jvmJson() {
        Map<String, Object> j = new LinkedHashMap<>();
        j.put("vm", System.getProperty("java.vm.name") + " " + System.getProperty("java.vm.version")
            + " (" + System.getProperty("java.vm.vendor") + ")");
        j.put("flags", ManagementFactory.getRuntimeMXBean().getInputArguments());
        List<Object> gc = new ArrayList<>();
        long totalCount = 0;
        long totalTime = 0;
        if (gcBeans != null) {
            for (int i = 0; i < gcBeans.size(); i++) {
                GarbageCollectorMXBean b = gcBeans.get(i);
                long c = b.getCollectionCount() - gcCountStart[i];
                long t = b.getCollectionTime() - gcTimeStart[i];
                Map<String, Object> e = new LinkedHashMap<>();
                e.put("name", b.getName());
                e.put("count", c);
                e.put("time_ms", t);
                gc.add(e);
                totalCount += c;
                totalTime += t;
            }
        }
        j.put("gc", gc);
        j.put("gc_total_count", totalCount);
        j.put("gc_total_time_ms", totalTime);
        CompilationMXBean comp = ManagementFactory.getCompilationMXBean();
        j.put("jit_compile_time_ms", comp != null && comp.isCompilationTimeMonitoringSupported()
            ? comp.getTotalCompilationTime() - jitStart : null);
        long heapPeak = -1;
        for (long[] x : samples()) {
            heapPeak = Math.max(heapPeak, x[3]);
        }
        j.put("heap_used_peak_bytes", heapPeak >= 0 ? heapPeak : null);
        j.put("heap_committed_bytes", memBean.getHeapMemoryUsage().getCommitted());
        return j;
    }

    // Buckets are indexed from the measured window start; RSS is the last sample at or before the bucket end.
    List<Object> timeseries(long windowStartNs, long[] buckets, double durationS) {
        int last = -1;
        for (int i = 0; i < buckets.length; i++) {
            if (buckets[i] != 0) {
                last = i;
            }
        }
        int n = Math.max(last + 1, (int) Math.ceil(durationS));
        n = Math.min(n, buckets.length);
        List<long[]> s = samples();
        List<Object> out = new ArrayList<>(n);
        int si = 0;
        long rss = s.isEmpty() ? -1 : s.get(0)[1];
        for (int b = 0; b < n; b++) {
            long bucketEnd = windowStartNs + (b + 1) * 1_000_000_000L;
            while (si < s.size() && s.get(si)[0] <= bucketEnd) {
                rss = s.get(si)[1];
                si++;
            }
            Map<String, Object> e = new LinkedHashMap<>();
            e.put("t", b + 1);
            e.put("msgs", buckets[b]);
            e.put("rss_bytes", rss >= 0 ? rss : null);
            out.add(e);
        }
        return out;
    }
}
