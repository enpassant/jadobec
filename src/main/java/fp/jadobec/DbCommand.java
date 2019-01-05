package fp.jadobec;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import fp.util.Either;
import fp.util.Failure;
import fp.util.Left;
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

    default <R, U> DbCommand<Stream<Either<Failure, R>>> mapList(
        Function<U, DbCommand<R>> mapper
    ) {
        return this.flatMap(items -> connection ->
             Right.of(((Stream<U>) items)
                .map(item -> mapper.apply(item).apply(connection))
        ));
    }

    default <R, U> DbCommand<Stream<Either<Failure, R>>> mapListEither(
        Function<U, DbCommand<R>> mapper
    ) {
        return this.flatMap(items -> connection ->
             Right.of(((Stream<Either<Failure, U>>) items)
                .map(item -> item.flatMap(
                    i -> mapper.apply(i).apply(connection))
                )
        ));
    }
}
