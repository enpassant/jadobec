package fp.io;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import fp.util.Failure;

public class Console {
    private Console() {
    }

    public static interface Service {
        IO<Object, Object, Void> println(String line);
        IO<Object, Failure, String> readLine();
    }

    public static interface Live extends Service {
        default IO<Object, Object, Void> println(String line) {
            return IO.effectTotal(() -> System.out.println(line));
        }
        default IO<Object, Failure, String> readLine() {
            return IO.effect(() -> {
                BufferedReader reader = new BufferedReader(
                    new InputStreamReader(System.in)
                );
                return reader.readLine();
            });
        }
    }

    private static class LiveImpl implements Live {
    }

    public static Service live() {
        return new LiveImpl();
    }

    public static IO<Service, Object, Void> println(String line) {
        return IO.accessM((Service console) -> console.println(line));
    }
    public static IO<Service, Failure, String> readLine() {
        return IO.accessM((Service console) -> console.readLine());
    }
}
