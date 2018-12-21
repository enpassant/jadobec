package fp.jadobec;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import fp.util.Either;
import fp.util.Failure;
import fp.util.Right;

public interface DbCommand<T> extends Function<Connection, Either<Failure, T>> {
    static <T> DbCommand<T> fix(T t) {
        return connection -> Right.of(t);
    }

    default <R> DbCommand<R> then(DbCommand<R> other) {
        return connection -> this.apply(connection).flatMap(
            t -> other.apply(connection)
        );
    }

    default DbCommand<T> with(Consumer<Connection> consumer) {
        return connection -> this.apply(connection).forEach(
            t-> consumer.accept(connection)
        );
    }

    default DbCommand<T> forEach(Consumer<T> consumer) {
        return connection -> this.apply(connection).forEach(consumer);
    }

    default <R> DbCommand<R> map(Function<T,R> mapper) {
        return connection -> this.apply(connection).map(mapper);
    }

    default <R> DbCommand<R> flatMap(Function<T, DbCommand<R>> mapper) {
        return connection -> this.apply(connection).flatMap(
            t -> mapper.apply(t).apply(connection)
        );
    }

    default <R> DbCommand<R> flatten() {
        return connection -> this.apply(connection).flatten();
    }

    default <R> DbCommand<R> recover(Function<Failure, R> recover) {
        return connection -> this.apply(connection).recover(recover);
    }

    default <R, U> DbCommand<List<Either<Failure, R>>> mapList(
        Function<U, DbCommand<R>> mapper
    ) {
        return this.flatMap(items -> connection ->
             Right.of(((List<U>) items).stream()
                .map(item -> mapper.apply(item).apply(connection))
                .collect(Collectors.toList())
        ));
    }

    default <R, U> DbCommand<List<R>> flatMapList(
        Function<U, DbCommand<R>> mapper
    ) {
        return this.flatMap(items -> connection -> {
            final List<R> result = new ArrayList<>();
            for (final U item : (List<U>) items) {
                final Either<Failure, R> mappedItem =
                    mapper.apply(item).apply(connection);
                if (mappedItem.left().isPresent()) {
                    return (Either<Failure, List<R>>) mappedItem;
                } else {
                    result.add(mappedItem.right().get());
                }
            }
            return Right.of(result);
        });
    }
}
