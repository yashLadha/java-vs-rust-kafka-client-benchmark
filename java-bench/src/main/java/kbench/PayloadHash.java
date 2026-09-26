package kbench;

import java.util.Map;
import java.util.Set;

final class PayloadHash {
    private PayloadHash() {
    }

    static void run(String[] argv, Map<String, Object> r) {
        Args a = Args.parse(argv, 1, Set.of("--payload", "--message-size", "--seed"), Set.of());
        String payload = a.choice("--payload", "random", "random", "text");
        int size = a.integer("--message-size", null);
        long seed = a.lng("--seed", 42L);
        Payload.Pool pool = Payload.build(payload, size, seed);
        r.put("payload_sha256", pool.sha256());
        r.put("pool_count", pool.count());
    }
}
