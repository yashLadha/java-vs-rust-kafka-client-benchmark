package kbench;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

final class Args {
    private final Map<String, String> values = new HashMap<>();
    private final Set<String> boolFlags;
    final Map<String, String> extra = new LinkedHashMap<>();

    private Args(Set<String> boolFlags) {
        this.boolFlags = boolFlags;
    }

    static Args parse(String[] argv, int from, Set<String> valueFlags, Set<String> boolFlags) {
        Args a = new Args(boolFlags);
        for (int i = from; i < argv.length; i++) {
            String f = argv[i];
            if (boolFlags.contains(f)) {
                a.values.put(f, "true");
            } else if (f.equals("--extra")) {
                String kv = next(argv, ++i, f);
                int eq = kv.indexOf('=');
                if (eq <= 0) {
                    throw new IllegalArgumentException("--extra expects k=v, got: " + kv);
                }
                a.extra.put(kv.substring(0, eq), kv.substring(eq + 1));
            } else if (valueFlags.contains(f)) {
                a.values.put(f, next(argv, ++i, f));
            } else {
                throw new IllegalArgumentException("unknown flag: " + f);
            }
        }
        return a;
    }

    private static String next(String[] argv, int i, String flag) {
        if (i >= argv.length) {
            throw new IllegalArgumentException(flag + " requires a value");
        }
        return argv[i];
    }

    String str(String flag, String def) {
        String v = values.get(flag);
        if (v == null) {
            if (def == null) {
                throw new IllegalArgumentException(flag + " is required");
            }
            return def;
        }
        return v;
    }

    String required(String flag) {
        return str(flag, null);
    }

    long lng(String flag, Long def) {
        String v = values.get(flag);
        if (v == null) {
            if (def == null) {
                throw new IllegalArgumentException(flag + " is required");
            }
            return def;
        }
        try {
            return Long.parseLong(v);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(flag + " expects an integer, got: " + v);
        }
    }

    int integer(String flag, Integer def) {
        long v = lng(flag, def == null ? null : def.longValue());
        if (v < Integer.MIN_VALUE || v > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(flag + " out of range: " + v);
        }
        return (int) v;
    }

    double dbl(String flag, double def) {
        String v = values.get(flag);
        if (v == null) {
            return def;
        }
        try {
            return Double.parseDouble(v);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(flag + " expects a number, got: " + v);
        }
    }

    boolean bool(String flag, boolean def) {
        String v = values.get(flag);
        if (v == null) {
            return def;
        }
        if (boolFlags.contains(flag)) {
            return true;
        }
        return switch (v) {
            case "true" -> true;
            case "false" -> false;
            default -> throw new IllegalArgumentException(flag + " expects true or false, got: " + v);
        };
    }

    String choice(String flag, String def, String... allowed) {
        String v = str(flag, def);
        for (String a : allowed) {
            if (a.equals(v)) {
                return v;
            }
        }
        throw new IllegalArgumentException(flag + " must be one of " + String.join(", ", allowed) + ", got: " + v);
    }
}
