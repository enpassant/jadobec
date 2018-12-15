package fp.jadobec;

import java.sql.Connection;
import java.util.function.Consumer;
import java.util.function.Function;

import fp.util.Either;
import fp.util.Failure;

public interface DbCommand<T> extends Function<Connection, Either<Failure, T>> {
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
}
