package fp.io;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import java.text.MessageFormat;

import fp.util.Failure;

public class Log {
    private Log() {
    }

    public static interface Service {
        IO<Object, Object, Void> debug(String message, Object... params);
    }

    public static class Live implements Service {
        public IO<Object, Object, Void> debug(String message, Object... params) {
            return IO.effectTotal(
                () -> System.out.println(
                    "[Debug] " + MessageFormat.format(message, params)
                )
            ).blocking();
        }
    }

    public static IO<Environment, Object, Void> debug(String message, Object... params) {
        return IO.accessM(env -> env.get(Service.class).debug(message, params));
    }
}
