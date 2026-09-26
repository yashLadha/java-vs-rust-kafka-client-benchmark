package kbench;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import org.HdrHistogram.Histogram;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

final class ConsumeMode {
    private static final Set<String> VALUE_FLAGS = Set.of(
        "--bootstrap", "--topic", "--partitions", "--num-messages", "--warmup-messages", "--fetch-min-bytes",
        "--fetch-max-wait-ms", "--max-partition-fetch-bytes", "--fetch-max-bytes", "--check-crcs",
        "--timeout-s", "--run-id", "--instances");
    private static final Set<String> BOOL_FLAGS = Set.of("--measure-e2e");
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(100);

    private ConsumeMode() {
    }

    private static final class Shared {
        final boolean e2e;
        final double timeoutS;
        final int nBuckets;
        final Resources res;
        final CyclicBarrier gate;
        final AtomicInteger remaining;
        volatile boolean stop;
        volatile long gateNs;
        volatile long deadlineNs;
        // Set once, under the lock, by the first instance to cross its warmup boundary.
        long t0;
        boolean windowStarted;

        Shared(boolean e2e, int instances, double timeoutS, Resources res) {
            this.e2e = e2e;
            this.timeoutS = timeoutS;
            this.nBuckets = (int) Math.ceil(timeoutS) + 2;
            this.res = res;
            this.remaining = new AtomicInteger(instances);
            this.gate = new CyclicBarrier(instances, () -> {
                gateNs = System.nanoTime();
                deadlineNs = gateNs + (long) (timeoutS * 1e9);
            });
        }

        synchronized long crossWarmup(long receivedNs) {
            if (!windowStarted) {
                windowStarted = true;
                res.start();
                t0 = System.nanoTime();
                return t0;
            }
            return Math.max(receivedNs, t0);
        }
    }

    private static final class Instance implements Runnable {
        final int k;
        final KafkaConsumer<byte[], byte[]> consumer;
        final List<TopicPartition> owned;
        final Shared sh;
        final long warmup;
        final long numMessages;
        final Histogram hist = Main.newHistogram();
        long[] buckets;
        long received;
        long measuredBytes;
        long touchedBytes;
        long firstNs;
        long startNs;
        long endNs;
        long t0;
        boolean timedOut;
        Throwable failure;

        Instance(int k, KafkaConsumer<byte[], byte[]> consumer, List<TopicPartition> owned, Shared sh,
                long warmup, long numMessages) {
            this.k = k;
            this.consumer = consumer;
            this.owned = owned;
            this.sh = sh;
            this.warmup = warmup;
            this.numMessages = numMessages;
            this.buckets = new long[sh.nBuckets];
        }

        long measured() {
            return Math.max(0, received - warmup);
        }

        @Override
        public void run() {
            try {
                Instances.await(sh.gate);
                loop();
            } catch (Throwable t) {
                failure = t;
                sh.stop = true;
            } finally {
                if (sh.remaining.decrementAndGet() == 0 && sh.res.started()) {
                    sh.res.stop();
                }
            }
        }

        private void loop() {
            // assign happens only after the gate because librdkafka starts fetching on assign; Java does the
            // same here so both harnesses start fetching at the same point.
            consumer.assign(owned);
            consumer.seekToBeginning(owned);
            long target = warmup + numMessages;
            if (numMessages == 0) {
                return;
            }
            outer:
            while (received < target) {
                if (sh.stop) {
                    return;
                }
                if (System.nanoTime() - sh.deadlineNs >= 0) {
                    timedOut = true;
                    sh.stop = true;
                    return;
                }
                ConsumerRecords<byte[], byte[]> records = consumer.poll(POLL_TIMEOUT);
                for (ConsumerRecord<byte[], byte[]> rec : records) {
                    long now = System.nanoTime();
                    byte[] v = rec.value();
                    int len = v == null ? 0 : v.length;
                    touchedBytes += len;
                    if (received == 0) {
                        firstNs = now;
                    }
                    if (received == warmup) {
                        now = sh.crossWarmup(now);
                        startNs = now;
                        t0 = sh.t0;
                    }
                    if (received >= warmup) {
                        measuredBytes += len;
                        int b = (int) Math.min((now - t0) / 1_000_000_000L, buckets.length - 1);
                        buckets[b]++;
                        if (sh.e2e && len >= 8) {
                            long ts = 0;
                            for (int j = 0; j < 8; j++) {
                                ts = (ts << 8) | (v[j] & 0xFFL);
                            }
                            long lat = (Main.epochNanos() - ts) / 1000L;
                            hist.recordValue(Math.max(0L, Math.min(lat, 120_000_000L)));
                        }
                        endNs = now;
                    }
                    received++;
                    if (received >= target) {
                        break outer;
                    }
                }
            }
        }
    }

    static void run(String[] argv, long mainStartNs, Map<String, Object> r) throws Exception {
        Args a = Args.parse(argv, 1, VALUE_FLAGS, BOOL_FLAGS);
        String bootstrap = a.str("--bootstrap", "kbench-kafka:9092");
        String topic = a.required("--topic");
        int partitions = a.integer("--partitions", null);
        long numMessages = a.lng("--num-messages", null);
        long warmup = a.lng("--warmup-messages", 0L);
        int fetchMinBytes = a.integer("--fetch-min-bytes", 1);
        int fetchMaxWaitMs = a.integer("--fetch-max-wait-ms", 500);
        int maxPartitionFetchBytes = a.integer("--max-partition-fetch-bytes", 1048576);
        int fetchMaxBytes = a.integer("--fetch-max-bytes", 52428800);
        boolean checkCrcs = a.bool("--check-crcs", true);
        boolean e2e = a.bool("--measure-e2e", false);
        double timeoutS = a.dbl("--timeout-s", 180);
        int instances = a.integer("--instances", 1);
        String runId = a.required("--run-id");
        r.put("run_id", runId);
        r.put("instances", instances);

        Map<String, Object> params = new LinkedHashMap<>();
        params.put("bootstrap", bootstrap);
        params.put("topic", topic);
        params.put("partitions", partitions);
        params.put("num_messages", numMessages);
        params.put("warmup_messages", warmup);
        params.put("fetch_min_bytes", fetchMinBytes);
        params.put("fetch_max_wait_ms", fetchMaxWaitMs);
        params.put("max_partition_fetch_bytes", maxPartitionFetchBytes);
        params.put("fetch_max_bytes", fetchMaxBytes);
        params.put("check_crcs", checkCrcs);
        params.put("measure_e2e", e2e);
        params.put("timeout_s", timeoutS);
        params.put("instances", instances);
        params.put("run_id", runId);
        params.put("extra", a.extra);
        r.put("params", params);

        if (partitions <= 0 || numMessages <= 0 || warmup < 0 || timeoutS <= 0) {
            throw new IllegalArgumentException("--partitions, --num-messages, --timeout-s must be > 0; --warmup-messages >= 0");
        }
        Instances.validate(instances, partitions);

        Map<String, String> eff = new LinkedHashMap<>();
        eff.put("bootstrap.servers", bootstrap);
        eff.put("group.id", runId);
        eff.put("enable.auto.commit", "false");
        eff.put("auto.offset.reset", "earliest");
        eff.put("fetch.min.bytes", Integer.toString(fetchMinBytes));
        eff.put("fetch.max.wait.ms", Integer.toString(fetchMaxWaitMs));
        eff.put("max.partition.fetch.bytes", Integer.toString(maxPartitionFetchBytes));
        eff.put("fetch.max.bytes", Integer.toString(fetchMaxBytes));
        eff.put("check.crcs", Boolean.toString(checkCrcs));
        eff.put("client.id", runId);
        eff.putAll(a.extra);
        r.put("effective_config", eff);

        Properties props = new Properties();
        props.putAll(eff);

        List<KafkaConsumer<byte[], byte[]>> consumers = new ArrayList<>(instances);
        try {
            for (int k = 0; k < instances; k++) {
                consumers.add(new KafkaConsumer<>(props, new ByteArrayDeserializer(), new ByteArrayDeserializer()));
            }
            Resources res = new Resources();
            Shared sh = new Shared(e2e, instances, timeoutS, res);
            List<Instance> insts = new ArrayList<>(instances);
            for (int k = 0; k < instances; k++) {
                List<TopicPartition> tps = new ArrayList<>();
                for (int p : Instances.owned(partitions, instances, k)) {
                    tps.add(new TopicPartition(topic, p));
                }
                insts.add(new Instance(k, consumers.get(k), tps, sh,
                    Instances.share(warmup, instances, k), Instances.share(numMessages, instances, k)));
            }
            r.put("startup_ms", Main.ms(System.nanoTime() - mainStartNs));
            Instances.runAll(insts, "kbench-consumer-");
            if (res.started()) {
                res.stop();
            }

            for (Instance in : insts) {
                if (in.failure != null) {
                    Log.error("instance " + in.k + " failed", in.failure);
                }
            }
            for (Instance in : insts) {
                if (in.failure != null) {
                    throw new IllegalStateException("instance " + in.k + ": " + in.failure, in.failure);
                }
            }

            long t0 = sh.t0;
            long tEnd = t0;
            long firstNs = 0;
            long received = 0;
            long measured = 0;
            long measuredBytes = 0;
            long touchedBytes = 0;
            boolean timedOut = false;
            Histogram hist = Main.newHistogram();
            long[] buckets = new long[sh.nBuckets];
            List<Object> details = new ArrayList<>(instances);
            for (Instance in : insts) {
                if (in.firstNs != 0 && (firstNs == 0 || in.firstNs - firstNs < 0)) {
                    firstNs = in.firstNs;
                }
                long m = in.measured();
                received += in.received;
                measured += m;
                measuredBytes += in.measuredBytes;
                touchedBytes += in.touchedBytes;
                timedOut |= in.timedOut;
                if (m > 0) {
                    tEnd = Math.max(tEnd, in.endNs);
                }
                hist.add(in.hist);
                for (int b = 0; b < buckets.length; b++) {
                    buckets[b] += in.buckets[b];
                }
                details.add(Instances.detail(in.k, m, 0, m > 0 ? in.endNs - in.startNs : 0, e2e ? in.hist : null));
            }
            if (firstNs != 0) {
                r.put("first_message_ms", Main.ms(firstNs - sh.gateNs));
            }
            long durNs = measured > 0 ? tEnd - t0 : 0;
            Main.fillThroughput(r, measured, measuredBytes, durNs);
            r.put("window_kind", instances == 1 ? "first_to_last" : "union");
            r.put("latency_kind", e2e ? "e2e" : null);
            r.put("latency_us", e2e ? Main.latencyJson(hist) : null);
            if (res.started()) {
                r.put("resources", res.resourcesJson(measured));
                r.put("jvm", res.jvmJson());
                r.put("timeseries", res.timeseries(t0, buckets, durNs / 1e9));
            }
            r.put("instances_detail", details);
            if (timedOut) {
                long target = warmup + numMessages;
                r.put("status", "timeout");
                r.put("error", "received " + received + " of " + target + " messages within " + timeoutS + " s");
            }
            Log.info("received=" + received + " measured=" + measured + " touched_bytes=" + touchedBytes);
        } finally {
            for (KafkaConsumer<byte[], byte[]> c : consumers) {
                try {
                    c.close();
                } catch (RuntimeException e) {
                    Log.warn("consumer close failed: " + e);
                }
            }
        }
    }
}
