package fp.jadobec;

import java.sql.Connection;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import fp.util.Either;
import fp.util.Right;

public interface DbCommand<F, T> extends Function<Connection, Either<F, T>> {
    static <F, T> DbCommand<F, T> fix(T t) {
        return connection -> Right.of(t);
    }

    default <R> DbCommand<F, R> then(DbCommand<F, R> other) {
        return connection -> this.apply(connection).flatMap(
            t -> other.apply(connection)
        );
    }

    default DbCommand<F, T> with(Consumer<Connection> consumer) {
        return connection -> this.apply(connection).forEach(
            t-> consumer.accept(connection)
        );
    }

    default DbCommand<F, T> forEach(Consumer<T> consumer) {
        return connection -> this.apply(connection).forEach(consumer);
    }

    default <R> DbCommand<F, R> map(Function<T,R> mapper) {
        return connection -> this.apply(connection).map(mapper);
    }

    default <R> DbCommand<F, R> flatMap(Function<T, DbCommand<F, R>> mapper) {
        return connection -> this.apply(connection).flatMap(
            t -> mapper.apply(t).apply(connection)
        );
    }

    default <R> DbCommand<F, R> flatten() {
        return connection -> this.apply(connection).flatten();
    }

    default <R> DbCommand<F, R> recover(Function<F, R> recover) {
        return connection -> this.apply(connection).recover(recover);
    }

    @SuppressWarnings("unchecked")
	default <R, U> DbCommand<F, Stream<Either<F, R>>> mapList(
        Function<U, DbCommand<F, R>> mapper
    ) {
        return this.flatMap(items -> connection ->
             Right.of(((Stream<U>) items)
                .map(item -> mapper.apply(item).apply(connection))
        ));
    }

    @SuppressWarnings("unchecked")
	default <R, U> DbCommand<F, Stream<Either<F, R>>> mapListEither(
        Function<U, DbCommand<F, R>> mapper
    ) {
        return this.flatMap(items -> connection ->
             Right.of(((Stream<Either<F, U>>) items)
                .map(item -> item.flatMap(
                    i -> mapper.apply(i).apply(connection))
                )
        ));
    }
}
