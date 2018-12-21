package fp.util;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class StreamUtil {
    public static <T, R> Function<List<T>, R> reduce(
        R init,
        BiFunction<R, T, R> fn
    ) {
        return items -> items.stream()
            .reduce(
                init,
                fn,
                (u1, u2) -> u1
            );
    }
}

