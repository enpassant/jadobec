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
        IO<Object, Object, Void> info(String message, Object... params);
    }

    public static class Live implements Service {
        private IO<Object, Object, Void> log(
            String level,
            String message,
            Object... params
        ) {
            return IO.effectTotal(
                () -> System.out.println(
                    "[" + level + "] " + MessageFormat.format(message, params)
                )
            ).blocking();
        }
        @Override
        public IO<Object, Object, Void> debug(String message, Object... params) {
            return log("Debug", message, params);
        }
        @Override
        public IO<Object, Object, Void> info(String message, Object... params) {
            return log("Info", message, params);
        }
    }

    public static IO<Environment, Object, Void> debug(String message, Object... params) {
        return IO.accessM(env -> env.get(Service.class).debug(message, params));
    }

    public static IO<Environment, Object, Void> info(String message, Object... params) {
        return IO.accessM(env -> env.get(Service.class).info(message, params));
    }
}
