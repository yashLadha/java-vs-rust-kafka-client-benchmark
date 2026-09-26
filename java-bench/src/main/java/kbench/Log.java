package kbench;

import java.time.LocalTime;

final class Log {
    private Log() {
    }

    static void info(String msg) {
        System.err.println(LocalTime.now() + " [kbench] " + msg);
    }

    static void warn(String msg) {
        System.err.println(LocalTime.now() + " [kbench] WARN " + msg);
    }

    static void error(String msg, Throwable t) {
        System.err.println(LocalTime.now() + " [kbench] ERROR " + msg);
        t.printStackTrace(System.err);
    }
}
