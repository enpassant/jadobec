package fp.util;

import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

public final class StreamUtil {
    public static <T, R> Function<Stream<T>, R> reduce(
        R init,
        BiFunction<R, T, R> fn
    ) {
        return items -> {
        	R result = items.reduce(
                init,
                fn,
                (u1, u2) -> u1
            );
        	items.close();
        	return result;
        };
    }

    public static <T, R> Function<Stream<T>, R> use(
            Function<Stream<T>, R> fn
        ) {
            return items -> {
            	R result = fn.apply(items);
            	items.close();
            	return result;
            };
        }
}

