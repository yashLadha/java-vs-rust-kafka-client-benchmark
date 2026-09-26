package kbench;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import org.HdrHistogram.Histogram;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;

final class ProduceMode {
    private static final Set<String> VALUE_FLAGS = Set.of(
        "--bootstrap", "--topic", "--partitions", "--num-messages", "--warmup-messages", "--max-duration-s",
        "--message-size", "--payload", "--seed", "--acks", "--compression", "--linger-ms", "--batch-size",
        "--max-in-flight", "--idempotence", "--buffer-memory", "--rate", "--run-id", "--instances");
    private static final Set<String> BOOL_FLAGS = Set.of("--embed-timestamp");
    private static final long MAX_BACKLOG_NS = 1_000_000L;
    // Acks can arrive up to delivery.timeout.ms after the last send, so the bucket array covers that too.
    private static final int EXTRA_BUCKETS = 130;

    private ProduceMode() {
    }

    // Written only by the owning producer's I/O thread; read by main after flush() returns and the thread joins.
    private static final class Measured {
        final Histogram hist = Main.newHistogram();
        final long[] buckets;
        final long t0;
        final AtomicLong acked = new AtomicLong();
        final AtomicLong errors = new AtomicLong();

        Measured(long t0, int nBuckets) {
            this.t0 = t0;
            this.buckets = new long[nBuckets];
        }
    }

    private static final class LatencyCallback implements Callback {
        private final Measured m;
        private final long startNs;

        LatencyCallback(Measured m, long startNs) {
            this.m = m;
            this.startNs = startNs;
        }

        @Override
        public void onCompletion(RecordMetadata md, Exception e) {
            long now = System.nanoTime();
            if (e != null) {
                m.errors.incrementAndGet();
                return;
            }
            m.hist.recordValue(Math.min((now - startNs) / 1000L, 120_000_000L));
            m.acked.incrementAndGet();
            int b = (int) Math.min((now - m.t0) / 1_000_000_000L, m.buckets.length - 1);
            m.buckets[b]++;
        }
    }

    private static final class Shared {
        final String topic;
        final byte[][] msgs;
        final boolean embedTs;
        final int instances;
        final long maxDurNs;
        final int nBuckets;
        final double ratePerInstance;
        final Resources res;
        final CyclicBarrier barrier;
        volatile boolean aborted;
        volatile long t0;

        Shared(String topic, byte[][] msgs, boolean embedTs, int instances, double maxDurationS, double rate,
               Resources res) {
            this.topic = topic;
            this.msgs = msgs;
            this.embedTs = embedTs;
            this.instances = instances;
            this.maxDurNs = (long) (maxDurationS * 1e9);
            this.nBuckets = (int) Math.ceil(maxDurationS) + EXTRA_BUCKETS;
            this.ratePerInstance = rate / instances;
            this.res = res;
            this.barrier = new CyclicBarrier(instances, () -> {
                if (!aborted) {
                    res.start();
                    t0 = System.nanoTime();
                }
            });
        }
    }

    private static final class Instance implements Runnable {
        final int k;
        final KafkaProducer<byte[], byte[]> producer;
        final Shared sh;
        final int[] owned;
        final long warmup;
        final long numMessages;
        Measured m;
        long sent;
        long syncErrors;
        long lagMaxNs;
        boolean truncated;
        long tEnd;
        Throwable failure;

        Instance(int k, KafkaProducer<byte[], byte[]> producer, Shared sh, int[] owned, long warmup, long numMessages) {
            this.k = k;
            this.producer = producer;
            this.sh = sh;
            this.owned = owned;
            this.warmup = warmup;
            this.numMessages = numMessages;
        }

        @Override
        public void run() {
            try {
                warmup();
            } catch (Throwable t) {
                failure = t;
                sh.aborted = true;
            }
            // Every instance arrives even after a failure so the others are never stranded at the barrier.
            Instances.await(sh.barrier);
            if (sh.aborted) {
                return;
            }
            try {
                measured();
            } catch (Throwable t) {
                failure = t;
            }
        }

        private ProducerRecord<byte[], byte[]> record(long j) {
            long i = j * sh.instances + k;
            byte[] v = value(sh.msgs[(int) (i % sh.msgs.length)], sh.embedTs);
            return new ProducerRecord<>(sh.topic, owned[(int) (j % owned.length)], null, v);
        }

        private void warmup() {
            if (warmup == 0) {
                return;
            }
            AtomicLong warmErrors = new AtomicLong();
            Callback warmCb = (md, e) -> {
                if (e != null) {
                    warmErrors.incrementAndGet();
                }
            };
            for (long j = 0; j < warmup; j++) {
                producer.send(record(j), warmCb);
            }
            producer.flush();
            if (warmErrors.get() > 0) {
                Log.warn("instance " + k + " warmup delivery errors: " + warmErrors.get());
            }
        }

        private void measured() {
            long t0 = sh.t0;
            m = new Measured(t0, sh.nBuckets);
            double intervalNs = sh.ratePerInstance > 0 ? 1e9 / sh.ratePerInstance : 0;
            long schedBaseNs = t0;
            long schedBaseN = 0;
            long n = 0;
            for (; n < numMessages; n++) {
                long now = System.nanoTime();
                if (now - t0 >= sh.maxDurNs) {
                    truncated = true;
                    break;
                }
                if (intervalNs > 0) {
                    long due = schedBaseNs + (long) ((n - schedBaseN) * intervalNs);
                    while (now < due) {
                        long rem = due - now;
                        if (rem > 200_000L) {
                            LockSupport.parkNanos(rem - 100_000L);
                        } else {
                            Thread.onSpinWait();
                        }
                        now = System.nanoTime();
                    }
                    long lag = now - due;
                    if (lag > lagMaxNs) {
                        lagMaxNs = lag;
                    }
                    // Cap catch-up bursts at 1 ms of backlog by re-anchoring the schedule.
                    if (lag > MAX_BACKLOG_NS) {
                        schedBaseNs = now - MAX_BACKLOG_NS;
                        schedBaseN = n;
                    }
                }
                ProducerRecord<byte[], byte[]> rec = record(warmup + n);
                long startNs = System.nanoTime();
                try {
                    producer.send(rec, new LatencyCallback(m, startNs));
                } catch (RuntimeException e) {
                    syncErrors++;
                    Log.warn("instance " + k + " send failed synchronously: " + e);
                }
            }
            sent = n;
            producer.flush();
            tEnd = System.nanoTime();
        }
    }

    static void run(String[] argv, long mainStartNs, Map<String, Object> r) throws Exception {
        Args a = Args.parse(argv, 1, VALUE_FLAGS, BOOL_FLAGS);
        String bootstrap = a.str("--bootstrap", "kbench-kafka:9092");
        String topic = a.required("--topic");
        int partitions = a.integer("--partitions", null);
        long numMessages = a.lng("--num-messages", null);
        long warmup = a.lng("--warmup-messages", 0L);
        double maxDurationS = a.dbl("--max-duration-s", 120);
        int messageSize = a.integer("--message-size", null);
        String payload = a.choice("--payload", "random", "random", "text");
        long seed = a.lng("--seed", 42L);
        String acks = a.choice("--acks", "1", "0", "1", "all");
        String compression = a.choice("--compression", "none", "none", "gzip", "snappy", "lz4", "zstd");
        long lingerMs = a.lng("--linger-ms", 5L);
        int batchSize = a.integer("--batch-size", 16384);
        int maxInFlight = a.integer("--max-in-flight", 5);
        boolean idempotence = a.bool("--idempotence", false);
        long bufferMemory = a.lng("--buffer-memory", 268435456L);
        double rate = a.dbl("--rate", 0);
        boolean embedTs = a.bool("--embed-timestamp", false);
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
        params.put("max_duration_s", maxDurationS);
        params.put("message_size", messageSize);
        params.put("payload", payload);
        params.put("seed", seed);
        params.put("acks", acks);
        params.put("compression", compression);
        params.put("linger_ms", lingerMs);
        params.put("batch_size", batchSize);
        params.put("max_in_flight", maxInFlight);
        params.put("idempotence", idempotence);
        params.put("buffer_memory", bufferMemory);
        params.put("rate", rate);
        params.put("embed_timestamp", embedTs);
        params.put("instances", instances);
        params.put("run_id", runId);
        params.put("extra", a.extra);
        r.put("params", params);

        if (partitions <= 0 || numMessages <= 0 || warmup < 0 || rate < 0 || maxDurationS <= 0) {
            throw new IllegalArgumentException("--partitions and --num-messages must be > 0; --warmup-messages, --rate >= 0; --max-duration-s > 0");
        }
        Instances.validate(instances, partitions);
        if (idempotence && !acks.equals("all")) {
            throw new IllegalArgumentException("--idempotence true requires --acks all");
        }
        if (embedTs && messageSize < 8) {
            throw new IllegalArgumentException("--embed-timestamp requires --message-size >= 8");
        }

        // Every instance gets this same map, so it is the full map passed to each client.
        Map<String, String> eff = new LinkedHashMap<>();
        eff.put("bootstrap.servers", bootstrap);
        eff.put("acks", acks);
        eff.put("linger.ms", Long.toString(lingerMs));
        eff.put("batch.size", Integer.toString(batchSize));
        eff.put("compression.type", compression);
        eff.put("max.in.flight.requests.per.connection", Integer.toString(maxInFlight));
        eff.put("enable.idempotence", Boolean.toString(idempotence));
        eff.put("buffer.memory", Long.toString(bufferMemory / instances));
        eff.put("max.block.ms", "60000");
        eff.put("retries", "2147483647");
        eff.put("delivery.timeout.ms", "120000");
        eff.put("request.timeout.ms", "30000");
        eff.put("max.request.size", "10485760");
        eff.put("client.id", runId);
        eff.putAll(a.extra);
        r.put("effective_config", eff);

        Properties props = new Properties();
        props.putAll(eff);

        List<KafkaProducer<byte[], byte[]>> producers = new ArrayList<>(instances);
        try {
            for (int k = 0; k < instances; k++) {
                producers.add(new KafkaProducer<>(props, new ByteArraySerializer(), new ByteArraySerializer()));
            }
            // Pool generation happens after startup_ms is captured so startup_ms measures runtime and
            // client initialization only, not the 64 MiB corpus build.
            r.put("startup_ms", Main.ms(System.nanoTime() - mainStartNs));

            Payload.Pool pool = Payload.build(payload, messageSize, seed);
            r.put("payload_sha256", pool.sha256());
            r.put("pool_count", pool.count());

            Resources res = new Resources();
            Shared sh = new Shared(topic, pool.messages(), embedTs, instances, maxDurationS, rate, res);
            List<Instance> insts = new ArrayList<>(instances);
            for (int k = 0; k < instances; k++) {
                insts.add(new Instance(k, producers.get(k), sh, Instances.owned(partitions, instances, k),
                    Instances.share(warmup, instances, k), Instances.share(numMessages, instances, k)));
            }
            Log.info("instances=" + instances + " warmup=" + warmup + " measured=" + numMessages + " messages");
            Instances.runAll(insts, "kbench-producer-");
            res.stop();

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
            long acked = 0;
            long errors = 0;
            long sent = 0;
            long lagMaxNs = 0;
            boolean truncated = false;
            Histogram hist = Main.newHistogram();
            long[] buckets = new long[sh.nBuckets];
            List<Object> details = new ArrayList<>(instances);
            for (Instance in : insts) {
                long ack = in.m.acked.get();
                long err = in.m.errors.get() + in.syncErrors;
                tEnd = Math.max(tEnd, in.tEnd);
                acked += ack;
                errors += err;
                sent += in.sent;
                lagMaxNs = Math.max(lagMaxNs, in.lagMaxNs);
                truncated |= in.truncated;
                hist.add(in.m.hist);
                for (int b = 0; b < buckets.length; b++) {
                    buckets[b] += in.m.buckets[b];
                }
                details.add(Instances.detail(in.k, ack, err, in.tEnd - t0, in.m.hist));
            }
            long durNs = tEnd - t0;
            Main.fillThroughput(r, acked, acked * (long) messageSize, durNs);
            r.put("errors", errors);
            r.put("truncated", truncated);
            r.put("window_kind", "barrier");
            r.put("rate_lag_max_us", rate > 0 ? lagMaxNs / 1000L : null);
            r.put("latency_kind", "send_to_ack");
            r.put("latency_us", Main.latencyJson(hist));
            r.put("resources", res.resourcesJson(acked));
            r.put("jvm", res.jvmJson());
            r.put("instances_detail", details);
            r.put("timeseries", res.timeseries(t0, buckets, durNs / 1e9));
            Log.info("sent=" + sent + " acked=" + acked + " errors=" + errors + " truncated=" + truncated);
        } finally {
            for (KafkaProducer<byte[], byte[]> p : producers) {
                try {
                    p.close();
                } catch (RuntimeException e) {
                    Log.warn("producer close failed: " + e);
                }
            }
        }
    }

    private static byte[] value(byte[] poolMsg, boolean embedTs) {
        if (!embedTs) {
            return poolMsg;
        }
        byte[] b = poolMsg.clone();
        long ts = Main.epochNanos();
        for (int j = 0; j < 8; j++) {
            b[j] = (byte) (ts >>> (56 - 8 * j));
        }
        return b;
    }
}
