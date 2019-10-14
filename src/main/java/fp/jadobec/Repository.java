package fp.jadobec;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.Stream.Builder;
import java.util.stream.StreamSupport;

import javax.sql.DataSource;

import fp.io.Environment;
import fp.io.Cause;
import fp.io.IO;
import fp.util.Either;
import fp.util.ExceptionFailure;
import fp.util.Failure;
import fp.util.GeneralFailure;
import fp.util.Left;
import fp.util.Right;
import fp.util.ThrowingConsumer;
import fp.util.ThrowingSupplier;
import fp.util.Tuple2;

public class Repository {
    public static interface Service {
        public <C, T> IO<C, Failure, T> use(
            IO<Connection, Failure, T> command
        );
    }

    public static class Live implements Service {
        private final ThrowingSupplier<Connection, SQLException> connectionFactory;

        private Live(final DataSource dataSource) {
            this.connectionFactory = () -> dataSource.getConnection();
        }

        @Override
        public <C, T> IO<C, Failure, T> use(
            IO<Connection, Failure, T> command
        ) {
            return IO.bracket(
                IO.effect(() -> connectionFactory.get()),
                connection -> IO.effect(() -> connection.close()),
                connection -> command.provide(connection)
            );
        }

        public static Either<Failure, Live> create(
            DataSource dataSource,
            String testSql
        ) {
            return ExceptionFailure.tryCatchFinal(
                () -> dataSource.getConnection().createStatement(),
                stmt -> {
                    ResultSet rs = stmt.executeQuery(testSql);
                    rs.close();
                    return new Live(dataSource);
                },
                stmt -> stmt.close()
            );
        }

        @SafeVarargs
        public static Either<Failure, Live> create(
            String driver,
            String testSql,
            Tuple2<String, String>... properties
        ) {
            Connection conn = null;
            Statement stmt = null;

            try {
                final Class<?> type = Class.forName(driver);
                final Constructor<?> constructor = type.getDeclaredConstructor();
                final DataSource dataSource = (DataSource) constructor.newInstance();
                for (final Tuple2<String, String> property : properties) {
                    final Method method = type.getDeclaredMethod(
                        "set" + property.getFirst(),
                        String.class
                    );
                    method.invoke(dataSource, property.getSecond());
                }

                conn = dataSource.getConnection();
                stmt = conn.createStatement();

                ResultSet rs = stmt.executeQuery(testSql);
                rs.close();
                return Right.of(new Live(dataSource));
            } catch (Exception e) {
                return Left.of(
                    ExceptionFailure.of(e)
                );
            } finally {
                try {
                    if (stmt != null) {
                        stmt.close();
                    }
                } catch (SQLException se) {
                }
            }
        }
    }

    public static <T> IO<Environment, Failure, T> use(
        IO<Connection, Failure, T> command
    ) {
        return IO.accessM(env -> env.get(Service.class).use(command));
    }

    public static <T> IO<Connection, Failure, T> querySingle(
        String sql,
        Extractor<T> createObject,
        Object... params
    ) {
        ThrowingConsumer<PreparedStatement, SQLException> prepare = ps -> {
            for (int i=0; i<params.length; i++) {
                ps.setObject(i + 1, params[i]);
            }
        };

        return querySinglePrepared(sql, prepare, createObject);
    }

    public static <T> IO<Connection, Failure, T> querySinglePrepared(
        String sql,
        ThrowingConsumer<PreparedStatement, SQLException> prepare,
        Extractor<T> createObject
    ) {
        return queryPrepared(sql, prepare, createObject, Repository::getFirstFromIterator);
    }

    private static <T> IO<Connection, Failure, T> getFirstFromIterator(
        Stream<T> stream
    ) {
        final Iterator<T> iterator = stream.iterator();
        if (iterator.hasNext()) {
            return IO.succeed(iterator.next());
        } else {
            return IO.fail(Cause.fail(GeneralFailure.of("Missing result")));
        }
    }

    public static <R, T> IO<Connection, Failure, R> query(
        String sql,
        Extractor<T> createObject,
        Function<Stream<T>, IO<Connection, Failure, R>> fn,
        Object... params
    ) {
        ThrowingConsumer<PreparedStatement, SQLException> prepare = ps -> {
            for (int i=0; i<params.length; i++) {
                ps.setObject(i + 1, params[i]);
            }
        };

        return queryPrepared(sql, prepare, createObject, fn);
    }

    public static <R, T> IO<Connection, Failure, R> queryPrepared(
        String sql,
        ThrowingConsumer<PreparedStatement, SQLException> prepare,
        Extractor<T> createObject,
        Function<Stream<T>, IO<Connection, Failure, R>> fn
    ) {
        return IO.bracket(IO.absolve(IO.access((Connection connection) -> {
            PreparedStatement stmt = null;

            try {
                stmt = connection.prepareStatement(sql);

                prepare.accept(stmt);

                ResultSet rs = stmt.executeQuery();
                return Right.of(stream(rs, createObject));
            } catch (Exception e) {
                return Left.of(
                    (Failure) ExceptionFailure.of(e)
                );
            }
        })),
            iterator -> IO.effect(() -> ((AutoCloseable) iterator).close()),
            fn
        ).blocking();
    }

    public static IO<Connection, Failure, Integer> update(final String sql) {
        return updatePrepared(sql, ps -> {});
    }

    public static IO<Connection, Failure, Integer> updatePrepared(
        final String sql,
        final ThrowingConsumer<PreparedStatement, SQLException> prepare
    ) {
        return IO.absolve(IO.access((Connection connection) -> {
            PreparedStatement stmt = null;

            try {
                stmt = connection.prepareStatement(
                    sql,
                    Statement.RETURN_GENERATED_KEYS
                );

                prepare.accept(stmt);

                stmt.executeUpdate();

                Right<Failure, Integer> result;

                try {
                    ResultSet generatedKeysRS = stmt.getGeneratedKeys();

                    result = Right.of(generatedKeysRS.next() ? generatedKeysRS.getInt(1) : 0);

                    generatedKeysRS.close();
                } catch(Exception e) {
                    result = Right.of(0);
                }

                return result;
            } catch (Exception e) {
                return Left.of(
                    (Failure) ExceptionFailure.of(e)
                );
            } finally {
                try {
                    if (stmt != null) stmt.close();
                } catch (SQLException e) {
                    //logger.error("Update prepared close error", e);
                }
            }
        })).blocking();
    }

    public static IO<Connection, Failure, Integer> batchUpdate(String... sqls) {
        return batchUpdateLoop(sqls, 0);
    }

    private static IO<Connection, Failure, Integer> batchUpdateLoop(
        String[] sqls,
        int index
    ) {
        return IO.<Connection, Failure, Boolean>succeed(
            sqls.length <= index
        ).flatMap((Boolean b) -> b ?
            IO.succeed((Integer) 0) :
            Repository.update(sqls[index])
                .flatMap(v -> batchUpdateLoop(sqls, index + 1))
        );
    }

    private static IO<Connection, Failure, Connection> setAutoCommit(
        Connection connection,
        boolean flag
    ) {
        return IO.absolve(IO.effectTotal(() ->
            ExceptionFailure.tryCatch(() -> {
                connection.setAutoCommit(flag);
                return connection;
            })
        ));
    }

    public static <T> IO<Connection, Failure, T> transaction(
        IO<Connection, Failure, T> dbCommand
    ) {
        return IO.access((Connection conn) -> conn).flatMap(
            connection -> IO.bracket(
                setAutoCommit(connection, false),
                connection2 -> setAutoCommit(connection, true),
                connection3 -> dbCommand.foldM(
                    failure -> {
                        try {
                            connection3.rollback();
                        } catch(SQLException e) {};
                        return IO.fail(Cause.fail(failure));
                    },
                    success -> {
                        try {
                            connection3.commit();
                        } catch(SQLException e) {};
                        return IO.succeed(success);
                    }
                )
            )
        ).blocking();
    }

    public static <T> IO<Connection, Failure, Stream<T>> mapToStream(
        Stream<T> stream
    ) {
        return IO.succeed(stream.collect(Collectors.toList()).stream());
    }

    public static <T> IO<Connection, Failure, Stream<T>> iterateToStreamWithFailure(
        Iterator<Either<Failure, T>> iterator
    ) {
        Builder<T> builder = Stream.builder();
        return iterateToStreamWithFailureLoop(builder, iterator);
    }

    private static <T> IO<Connection, Failure, Stream<T>> iterateToStreamWithFailureLoop(
        Builder<T> builder,
        Iterator<Either<Failure, T>> iterator
    ) {
        return IO.<Connection, Failure, Boolean>succeed(
            iterator.hasNext()
        ).flatMap(hasNext -> {
            if (hasNext) {
                Either<Failure, T> value = iterator.next();
                if (value.isRight()) {
                    builder.accept(value.right());
                    return iterateToStreamWithFailureLoop(builder, iterator);
                } else {
                    return IO.fail(Cause.fail(value.left()));
                }
            } else {
                return IO.succeed(builder.build());
            }
        });
    }

    public static <T> IO<Connection, Failure, List<T>> mapToList(
        Stream<T> stream
    ) {
        return IO.succeed(stream.collect(Collectors.toList()));
    }

    public static <F, R, U> IO<Connection, F, Stream<Either<F, R>>> mapStreamEither(
        IO<Connection, F, Stream<Either<F, U>>> io,
        Function<U, IO<Connection, F, R>> mapper
    ) {
        Builder<Either<F, R>> builder = Stream.builder();
        return io.flatMap(stream -> mapStreamEitherLoop(builder, stream.iterator(), mapper));
    }

    private static <F, R, U> IO<Connection, F, Stream<Either<F, R>>> mapStreamEitherLoop(
        Builder<Either<F, R>> builder,
        Iterator<Either<F, U>> iterator,
        Function<U, IO<Connection, F, R>> mapper
    ) {
        return IO.<Connection, F, Boolean>succeed(
            iterator.hasNext()
        ).flatMap(hasNext -> {
            if (hasNext) {
                return iterator.next().map(mapper).fold(
                    failure -> {
                        builder.accept(Left.of(failure));
                        return mapStreamEitherLoop(builder, iterator, mapper);
                    },
                    success -> success.either().flatMap(value -> {
                        builder.accept(value);
                        return mapStreamEitherLoop(builder, iterator, mapper);
                    }));
            } else {
                return IO.succeed(builder.build());
            }
        });
    }

    private static <T> Stream<T> stream(
        final ResultSet resultSet,
        final Extractor<T> extractor
    ) {
        ResultSetIterator<T> iterator = new ResultSetIterator<T>(resultSet, extractor);
        return (Stream<T>) StreamSupport.stream(
            Spliterators.spliteratorUnknownSize(iterator, 0),
            false
        ).onClose(() -> {
            try {
                iterator.close();
            } catch (Exception e) {
            }
        });
    }

    private static class ResultSetIterator<T>
        implements Iterator<T>, AutoCloseable
    {
        private boolean stepNext = true;
        private boolean lastHasNext = false;
        private final ResultSet resultSet;
        private final Extractor<T> extractor;

        public ResultSetIterator(final ResultSet resultSet, final Extractor<T> extractor) {
            this.extractor = extractor;
            this.resultSet = resultSet;
        }

        @Override
        public boolean hasNext() {
            try {
                if (stepNext) {
                    stepNext = false;
                    lastHasNext = resultSet.next();
                    return lastHasNext;
                } else {
                    return lastHasNext;
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public T next() {
            try {
                stepNext = true;
                return extractor.extract(resultSet);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void close() throws SQLException {
            resultSet.getStatement().close();
            resultSet.close();
        }
    }
}
