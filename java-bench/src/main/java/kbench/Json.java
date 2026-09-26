package kbench;

import java.util.Collection;
import java.util.Map;

final class Json {
    private Json() {
    }

    static String write(Object value) {
        StringBuilder sb = new StringBuilder(4096);
        write(value, sb);
        return sb.toString();
    }

    private static void write(Object v, StringBuilder sb) {
        if (v == null) {
            sb.append("null");
        } else if (v instanceof String s) {
            writeString(s, sb);
        } else if (v instanceof Boolean b) {
            sb.append(b.booleanValue());
        } else if (v instanceof Double d) {
            writeDouble(d, sb);
        } else if (v instanceof Float f) {
            writeDouble(f.doubleValue(), sb);
        } else if (v instanceof Number n) {
            sb.append(n.longValue());
        } else if (v instanceof Map<?, ?> m) {
            sb.append('{');
            boolean first = true;
            for (Map.Entry<?, ?> e : m.entrySet()) {
                if (!first) {
                    sb.append(',');
                }
                first = false;
                writeString(String.valueOf(e.getKey()), sb);
                sb.append(':');
                write(e.getValue(), sb);
            }
            sb.append('}');
        } else if (v instanceof Collection<?> c) {
            sb.append('[');
            boolean first = true;
            for (Object o : c) {
                if (!first) {
                    sb.append(',');
                }
                first = false;
                write(o, sb);
            }
            sb.append(']');
        } else {
            writeString(v.toString(), sb);
        }
    }

    private static void writeDouble(double d, StringBuilder sb) {
        if (Double.isNaN(d) || Double.isInfinite(d)) {
            sb.append("null");
        } else {
            sb.append(d);
        }
    }

    private static void writeString(String s, StringBuilder sb) {
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        sb.append('"');
    }
}
