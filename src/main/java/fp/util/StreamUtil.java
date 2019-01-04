package fp.util;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class StreamUtil {
    public static <T, R> Function<Stream<T>, R> reduce(
        R init,
        BiFunction<R, T, R> fn
    ) {
        return items -> items
            .reduce(
                init,
                fn,
                (u1, u2) -> u1
            );
    }
}

