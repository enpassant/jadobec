package jadobec;

import java.sql.Connection;
import java.util.function.Function;

import util.Either;
import util.Failure;

public interface DbCommand<T> extends Function<Connection, Either<Failure, T>> {
}
