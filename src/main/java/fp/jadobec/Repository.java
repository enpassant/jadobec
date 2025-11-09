package fp.jadobec;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterators;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.Stream.Builder;
import java.util.stream.StreamSupport;

import javax.sql.DataSource;

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

public class Repository
{
    public final String name;

    private Repository()
    {
        this.name = UUID.randomUUID().toString();
    }

    public static Repository of()
    {
        return new Repository();
    }

    public interface Service
    {
        <C, T> IO<Failure, T> use(final IO<Failure, T> command);

        <T> IO<Failure, T> querySingle(
            String sql,
            Extractor<T> createObject,
            Object... params
        );

        <T> IO<Failure, T> querySinglePrepared(
            String sql,
            ThrowingConsumer<PreparedStatement, SQLException> prepare,
            Extractor<T> createObject
        );

        <R, T> IO<Failure, R> query(
            String sql,
            Extractor<T> createObject,
            Function<Stream<T>, IO<Failure, R>> fn,
            Object... params
        );

        <R, T> IO<Failure, R> queryPrepared(
            String sql,
            ThrowingConsumer<PreparedStatement, SQLException> prepare,
            Extractor<T> createObject,
            Function<Stream<T>, IO<Failure, R>> fn
        );

        IO<Failure, Integer> update(
            final String sql,
            Object... params
        );

        IO<Failure, Integer> updatePrepared(
            final String sql,
            final ThrowingConsumer<PreparedStatement, SQLException> prepare
        );

        IO<Failure, Integer> batchUpdate(String... sqls);

        <T> IO<Failure, T> transaction(
            IO<Failure, T> dbCommand
        );

        <T> IO<Failure, Stream<T>> mapToStream(
            Stream<T> stream
        );

        <T> IO<Failure, Stream<T>> iterateToStreamWithFailure(
            Iterator<Either<Failure, T>> iterator
        );

        <T> IO<Failure, List<T>> mapToList(
            Stream<T> stream
        );

        <F, R, U> IO<F, Stream<Either<F, R>>> mapStreamEither(
            IO<F, Stream<Either<F, U>>> io,
            Function<U, IO<F, R>> mapper
        );
    }

    public static class Live implements Service
    {
        private final String name;

        private final ThrowingSupplier<Connection, SQLException> connectionFactory;

        private Live(final DataSource dataSource)
        {
            this.name = UUID.randomUUID().toString();
            this.connectionFactory = () -> dataSource.getConnection();
        }

        @Override
        public <C, T> IO<Failure, T> use(
            final IO<Failure, T> command
        )
        {
            return IO.bracket(
                IO.effect(() -> connectionFactory.get()),
                connection -> IO.effect(() -> connection.close()),
                connection -> command.provide(name, Connection.class, connection)
            );
        }

        public static Either<Failure, Live> create(
            DataSource dataSource,
            String testSql
        )
        {
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
        )
        {
            Connection conn = null;
            Statement stmt = null;

            try {
                final Class<?> type = Class.forName(driver);
                final Constructor<?> constructor = type.getDeclaredConstructor();
                final DataSource dataSource = (DataSource) constructor.newInstance();
                for (final Tuple2<String, String> property : properties) {
                    final Method method = type.getMethod(
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

        public <T> IO<Failure, T> querySingle(
            String sql,
            Extractor<T> createObject,
            Object... params
        )
        {
            ThrowingConsumer<PreparedStatement, SQLException> prepare = ps -> {
                for (int i = 0; i < params.length; i++) {
                    ps.setObject(i + 1, params[i]);
                }
            };

            return querySinglePrepared(sql, prepare, createObject);
        }

        public <T> IO<Failure, T> querySinglePrepared(
            String sql,
            ThrowingConsumer<PreparedStatement, SQLException> prepare,
            Extractor<T> createObject
        )
        {
            return queryPrepared(sql, prepare, createObject, this::getFirstFromStream);
        }

        private <T> IO<Failure, T> getFirstFromStream(
            Stream<T> stream
        )
        {
            final Iterator<T> iterator = stream.iterator();
            if (iterator.hasNext()) {
                return IO.succeed(iterator.next());
            } else {
                return IO.fail(Cause.fail(GeneralFailure.of("Missing result")));
            }
        }

        public <R, T> IO<Failure, R> query(
            String sql,
            Extractor<T> createObject,
            Function<Stream<T>, IO<Failure, R>> fn,
            Object... params
        )
        {
            ThrowingConsumer<PreparedStatement, SQLException> prepare = ps -> {
                for (int i = 0; i < params.length; i++) {
                    ps.setObject(i + 1, params[i]);
                }
            };

            return queryPrepared(sql, prepare, createObject, fn);
        }

        public <R, T> IO<Failure, R> queryPrepared(
            String sql,
            ThrowingConsumer<PreparedStatement, SQLException> prepare,
            Extractor<T> createObject,
            Function<Stream<T>, IO<Failure, R>> fn
        )
        {
            return IO.bracket(IO.absolve(IO.access(name, Connection.class, connection -> {
                    PreparedStatement stmt = null;

                    try {
                        stmt = connection.prepareStatement(sql);

                        prepare.accept(stmt);

                        ResultSet rs = stmt.executeQuery();
                        return Right.of(stream(rs, createObject));
                    } catch (Exception e) {
                        return Left.of(
                            ExceptionFailure.of(e)
                        );
                    }
                })),
                iterator -> IO.effect(() -> ((AutoCloseable) iterator).close()),
                fn
            ).blocking();
        }

        public IO<Failure, Integer> update(
            final String sql,
            Object... params
        )
        {
            ThrowingConsumer<PreparedStatement, SQLException> prepare = ps -> {
                for (int i = 0; i < params.length; i++) {
                    ps.setObject(i + 1, params[i]);
                }
            };

            return updatePrepared(sql, prepare);
        }

        public IO<Failure, Integer> updatePrepared(
            final String sql,
            final ThrowingConsumer<PreparedStatement, SQLException> prepare
        )
        {
            return IO.absolve(IO.access(name, Connection.class, connection -> {
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
                    } catch (Exception e) {
                        result = Right.of(0);
                    }

                    return result;
                } catch (Exception e) {
                    return Left.of(
                        ExceptionFailure.of(e)
                    );
                } finally {
                    try {
                        if (stmt != null) {
                            stmt.close();
                        }
                    } catch (SQLException e) {
                        //logger.error("Update prepared close error", e);
                    }
                }
            })).blocking();
        }

        public IO<Failure, Integer> batchUpdate(String... sqls)
        {
            return batchUpdateLoop(sqls, 0);
        }

        private IO<Failure, Integer> batchUpdateLoop(
            String[] sqls,
            int index
        )
        {
            return IO.<Failure, Boolean>succeed(
                sqls.length <= index
            ).flatMap((Boolean b) -> b ?
                IO.succeed(0) :
                update(sqls[index])
                    .flatMap(v -> batchUpdateLoop(sqls, index + 1))
            );
        }

        private IO<Failure, Connection> setAutoCommit(
            Connection connection,
            boolean flag
        )
        {
            return IO.absolve(IO.effectTotal(() ->
                ExceptionFailure.tryCatch(() -> {
                    connection.setAutoCommit(flag);
                    return connection;
                })
            ));
        }

        public <T> IO<Failure, T> transaction(
            IO<Failure, T> dbCommand
        )
        {
            return IO.access(name, Connection.class, conn -> conn).flatMap(
                connection -> IO.bracket(
                    setAutoCommit(connection, false),
                    connection2 -> setAutoCommit(connection, true),
                    connection3 -> dbCommand.peekM(t ->
                        IO.effect(() -> connection3.commit())
                    ).recover(failure -> this.<T>rollback(connection3, failure))
                )
            ).blocking();
        }

        private <T> IO<Failure, T> rollback(
            Connection connection,
            Failure failure
        )
        {
            try {
                connection.rollback();
            } catch (SQLException e) {
                return IO.fail(
                    Cause.fail(failure)
                        .then(Cause.fail(ExceptionFailure.of(e)))
                );
            }
            return IO.fail(Cause.fail(failure));
        }

        public <T> IO<Failure, Stream<T>> mapToStream(
            Stream<T> stream
        )
        {
            return IO.succeed(stream.collect(Collectors.toList()).stream());
        }

        public <T> IO<Failure, Stream<T>> iterateToStreamWithFailure(
            Iterator<Either<Failure, T>> iterator
        )
        {
            Builder<T> builder = Stream.builder();
            return iterateToStreamWithFailureLoop(builder, iterator);
        }

        private <T> IO<Failure, Stream<T>> iterateToStreamWithFailureLoop(
            Builder<T> builder,
            Iterator<Either<Failure, T>> iterator
        )
        {
            return IO.<Failure, Boolean>succeed(
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

        public <T> IO<Failure, List<T>> mapToList(
            Stream<T> stream
        )
        {
            return IO.succeed(stream.collect(Collectors.toList()));
        }

        public <F, R, U> IO<F, Stream<Either<F, R>>> mapStreamEither(
            IO<F, Stream<Either<F, U>>> io,
            Function<U, IO<F, R>> mapper
        )
        {
            Builder<Either<F, R>> builder = Stream.builder();
            return io.flatMap(stream -> mapStreamEitherLoop(builder, stream.iterator(), mapper));
        }

        private <F, R, U> IO<F, Stream<Either<F, R>>> mapStreamEitherLoop(
            Builder<Either<F, R>> builder,
            Iterator<Either<F, U>> iterator,
            Function<U, IO<F, R>> mapper
        )
        {
            return IO.<F, Boolean>succeed(
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

        private <T> Stream<T> stream(
            final ResultSet resultSet,
            final Extractor<T> extractor
        )
        {
            Live.ResultSetIterator<T> iterator = new Live.ResultSetIterator<T>(resultSet, extractor);
            return StreamSupport.stream(
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

            public ResultSetIterator(final ResultSet resultSet, final Extractor<T> extractor)
            {
                this.extractor = extractor;
                this.resultSet = resultSet;
            }

            @Override
            public boolean hasNext()
            {
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
            public T next()
            {
                try {
                    stepNext = true;
                    return extractor.extract(resultSet);
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void close() throws SQLException
            {
                resultSet.getStatement().close();
                resultSet.close();
            }
        }
    }

    public <T> IO<Failure, T> use(
        IO<Failure, T> command
    )
    {
        return IO.accessM(this.name, Service.class, env -> env.use(command));
    }

    public <T> IO<Failure, T> querySingle(
        String sql,
        Extractor<T> createObject,
        Object... params
    )
    {
        return IO.accessM(
            this.name,
            Service.class,
            env -> env.querySingle(sql, createObject, params)
        );
    }

    public <T> IO<Failure, T> querySinglePrepared(
        String sql,
        ThrowingConsumer<PreparedStatement, SQLException> prepare,
        Extractor<T> createObject
    )
    {
        return IO.accessM(
            this.name,
            Service.class,
            env -> env.querySinglePrepared(sql, prepare, createObject)
        );
    }

    public <R, T> IO<Failure, R> query(
        String sql,
        Extractor<T> createObject,
        Function<Stream<T>, IO<Failure, R>> fn,
        Object... params
    )
    {
        return IO.accessM(
            this.name,
            Service.class,
            env -> env.query(sql, createObject, fn, params)
        );
    }

    public <R, T> IO<Failure, R> queryPrepared(
        String sql,
        ThrowingConsumer<PreparedStatement, SQLException> prepare,
        Extractor<T> createObject,
        Function<Stream<T>, IO<Failure, R>> fn
    )
    {
        return IO.accessM(
            this.name,
            Service.class,
            env -> env.queryPrepared(sql, prepare, createObject, fn)
        );
    }

    public IO<Failure, Integer> update(
        final String sql,
        Object... params
    )
    {
        return IO.accessM(
            this.name,
            Service.class,
            env -> env.update(sql, params)
        );
    }

    public IO<Failure, Integer> updatePrepared(
        final String sql,
        final ThrowingConsumer<PreparedStatement, SQLException> prepare
    )
    {
        return IO.accessM(
            this.name,
            Service.class,
            env -> env.updatePrepared(sql, prepare)
        );
    }

    public IO<Failure, Integer> batchUpdate(String... sqls)
    {
        return IO.accessM(
            this.name,
            Service.class,
            env -> env.batchUpdate(sqls)
        );
    }

    public <T> IO<Failure, T> transaction(
        IO<Failure, T> dbCommand
    )
    {
        return IO.accessM(
            this.name,
            Service.class,
            env -> env.transaction(dbCommand)
        );
    }

    public <T> IO<Failure, Stream<T>> mapToStream(
        Stream<T> stream
    )
    {
        return IO.accessM(
            this.name,
            Service.class,
            env -> env.mapToStream(stream)
        );
    }

    public <T> IO<Failure, Stream<T>> iterateToStreamWithFailure(
        Iterator<Either<Failure, T>> iterator
    )
    {
        return IO.accessM(
            this.name,
            Service.class,
            env -> env.iterateToStreamWithFailure(iterator)
        );
    }

    public <T> IO<Failure, List<T>> mapToList(
        Stream<T> stream
    )
    {
        return IO.accessM(
            this.name,
            Service.class,
            env -> env.mapToList(stream)
        );
    }

    public <F, R, U> IO<F, Stream<Either<F, R>>> mapStreamEither(
        IO<F, Stream<Either<F, U>>> io,
        Function<U, IO<F, R>> mapper
    )
    {
        return IO.accessM(
            this.name,
            Service.class,
            env -> env.mapStreamEither(io, mapper)
        );
    }
}
