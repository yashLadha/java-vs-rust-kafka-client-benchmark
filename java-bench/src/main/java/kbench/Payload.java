package kbench;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HexFormat;

final class Payload {
    private static final String[] WORDS = {
        "kafka", "stream", "broker", "topic", "partition", "offset", "consumer", "producer",
        "record", "batch", "leader", "follower", "replica", "commit", "segment", "index",
        "latency", "throughput", "cluster", "message", "payload", "header", "key", "value",
        "timestamp", "schema", "event", "log", "queue", "fetch", "poll", "ack"
    };
    private static final byte[][] WORD_BYTES = new byte[WORDS.length][];
    private static final int MAX_WORD_LEN;

    static {
        int max = 0;
        for (int i = 0; i < WORDS.length; i++) {
            WORD_BYTES[i] = WORDS[i].getBytes(StandardCharsets.US_ASCII);
            max = Math.max(max, WORD_BYTES[i].length);
        }
        MAX_WORD_LEN = max;
    }

    record Pool(byte[][] messages, String sha256) {
        int count() {
            return messages.length;
        }
    }

    private Payload() {
    }

    static final class SplitMix64 {
        private long state;

        SplitMix64(long seed) {
            this.state = seed;
        }

        // Java long arithmetic wraps mod 2^64 and >>> is the logical shift, which matches u64 semantics.
        long next() {
            state += 0x9E3779B97F4A7C15L;
            long z = state;
            z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
            z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
            return z ^ (z >>> 31);
        }
    }

    static int poolCount(int messageSize) {
        return (int) Math.min(16384L, 67108864L / messageSize);
    }

    static Pool build(String kind, int messageSize, long seed) {
        if (messageSize <= 0) {
            throw new IllegalArgumentException("--message-size must be > 0");
        }
        int count = poolCount(messageSize);
        if (count <= 0) {
            throw new IllegalArgumentException("--message-size too large for a 64 MiB pool");
        }
        SplitMix64 rng = new SplitMix64(seed);
        byte[][] pool = new byte[count][];
        for (int m = 0; m < count; m++) {
            pool[m] = switch (kind) {
                case "random" -> randomMessage(rng, messageSize);
                case "text" -> textMessage(rng, messageSize);
                default -> throw new IllegalArgumentException("--payload must be random or text");
            };
        }
        return new Pool(pool, sha256(pool));
    }

    private static byte[] randomMessage(SplitMix64 rng, int size) {
        byte[] b = new byte[size];
        for (int off = 0; off < size; off += 8) {
            long v = rng.next();
            int n = Math.min(8, size - off);
            for (int k = 0; k < n; k++) {
                b[off + k] = (byte) (v >>> (8 * k));
            }
        }
        return b;
    }

    private static byte[] textMessage(SplitMix64 rng, int size) {
        byte[] buf = new byte[size + MAX_WORD_LEN + 1];
        int len = 0;
        while (len < size) {
            byte[] w = WORD_BYTES[(int) Long.remainderUnsigned(rng.next(), WORDS.length)];
            System.arraycopy(w, 0, buf, len, w.length);
            len += w.length;
            buf[len++] = ' ';
        }
        return Arrays.copyOf(buf, size);
    }

    private static String sha256(byte[][] pool) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            for (byte[] m : pool) {
                md.update(m);
            }
            return HexFormat.of().formatHex(md.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
