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
import java.util.stream.Stream;
import java.util.stream.Stream.Builder;

import javax.sql.DataSource;

import fp.io.Environment;
import fp.io.IO;
import fp.io.Runtime;
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
        public IO<Object, Failure, Connection> getConnection();

        public <T> IO<Object, Failure, T> querySingle(
            Connection connection,
            String sql,
            Extractor<T> createObject,
            Object... params
        );

        public <T> IO<Object, Failure, T> querySinglePrepared(
            Connection connection,
            String sql,
            ThrowingConsumer<PreparedStatement, SQLException> prepare,
            Extractor<T> createObject
        );

        public <R, T> IO<Object, Failure, R> query(
            Connection connection,
            String sql,
            Extractor<T> createObject,
            Function<Iterator<T>, IO<Object, Failure, R>> fn,
            Object... params
        );

        public <R, T> IO<Object, Failure, R> queryPrepared(
            Connection connection,
            String sql,
            ThrowingConsumer<PreparedStatement, SQLException> prepare,
            Extractor<T> createObject,
            Function<Iterator<T>, IO<Object, Failure, R>> fn
        );

        public IO<Object, Failure, Integer> update(
            Connection connection,
            final String sql
        );

        public IO<Object, Failure, Integer> updatePrepared(
            Connection connection,
            final String sql,
            final ThrowingConsumer<PreparedStatement, SQLException> prepare
        );

        public IO<Object, Failure, Integer> batchUpdate(
            Connection connection,
            String... sqls
        );

        public <T> IO<Object, Failure, T> transaction(
            Connection connection,
            IO<Object, Failure, T> dbCommand
        );
    }

    public static class Live implements Service {
        private final ThrowingSupplier<Connection, SQLException> connectionFactory;

        private Live(final DataSource dataSource) {
            this.connectionFactory = () -> dataSource.getConnection();
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

        @Override
        public IO<Object, Failure, Connection> getConnection() {
            return IO.effect(() -> connectionFactory.get());
        }

        @Override
        public <T> IO<Object, Failure, T> querySingle(
            Connection connection,
            String sql,
            Extractor<T> createObject,
            Object... params
        ) {
            ThrowingConsumer<PreparedStatement, SQLException> prepare = ps -> {
                for (int i=0; i<params.length; i++) {
                    ps.setObject(i + 1, params[i]);
                }
            };

            return querySinglePrepared(connection, sql, prepare, createObject);
        }

        @Override
        public <T> IO<Object, Failure, T> querySinglePrepared(
            Connection connection,
            String sql,
            ThrowingConsumer<PreparedStatement, SQLException> prepare,
            Extractor<T> createObject
        ) {
            return queryPrepared(
                connection,
                sql,
                prepare,
                createObject,
                Repository.Live::getFirstFromIterator
            );
        }

        private static <T> IO<Object, Failure, T> getFirstFromIterator(
            Iterator<T> iterator
        ) {
            if (iterator.hasNext()) {
                return IO.succeed(iterator.next());
            } else {
                return IO.fail(GeneralFailure.of("Missing result"));
            }
        }

        @Override
        public <R, T> IO<Object, Failure, R> query(
            Connection connection,
            String sql,
            Extractor<T> createObject,
            Function<Iterator<T>, IO<Object, Failure, R>> fn,
            Object... params
        ) {
            ThrowingConsumer<PreparedStatement, SQLException> prepare = ps -> {
                for (int i=0; i<params.length; i++) {
                    ps.setObject(i + 1, params[i]);
                }
            };

            return queryPrepared(connection, sql, prepare, createObject, fn);
        }

        @Override
        public <R, T> IO<Object, Failure, R> queryPrepared(
            Connection connection,
            String sql,
            ThrowingConsumer<PreparedStatement, SQLException> prepare,
            Extractor<T> createObject,
            Function<Iterator<T>, IO<Object, Failure, R>> fn
        ) {
            return IO.bracket(IO.absolve(IO.effect(() -> {
                PreparedStatement stmt = null;

                try {
                    stmt = connection.prepareStatement(sql);

                    prepare.accept(stmt);

                    ResultSet rs = stmt.executeQuery();
                    return Right.of((Iterator<T>) new ResultSetIterator<T>(rs, createObject));
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

        @Override
        public IO<Object, Failure, Integer> update(
            Connection connection,
            final String sql
        ) {
            return updatePrepared(connection, sql, ps -> {});
        }

        @Override
        public IO<Object, Failure, Integer> updatePrepared(
            Connection connection,
            final String sql,
            final ThrowingConsumer<PreparedStatement, SQLException> prepare
        ) {
            return IO.absolve(IO.effect(() -> {
                PreparedStatement stmt = null;

                try {
                    stmt = connection.prepareStatement(
                        sql,
                        Statement.RETURN_GENERATED_KEYS
                    );

                    prepare.accept(stmt);

                    stmt.executeUpdate();

                    ResultSet generatedKeysRS = stmt.getGeneratedKeys();

                    Right<Failure, Integer> result =
                        Right.of(generatedKeysRS.next() ? generatedKeysRS.getInt(1) : 0);

                    generatedKeysRS.close();

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

        @Override
        public IO<Object, Failure, Integer> batchUpdate(
            Connection connection,
            String... sqls
        ) {
            return batchUpdateLoop(connection, sqls, 0);
        }

        private IO<Object, Failure, Integer> batchUpdateLoop(
            Connection connection,
            String[] sqls,
            int index
        ) {
            return IO.<Object, Failure, Boolean>succeed(
                sqls.length <= index
            ).flatMap((Boolean b) -> b ?
                IO.succeed((Integer) 0) :
                update(connection, sqls[index])
                    .flatMap(v -> batchUpdateLoop(connection, sqls, index + 1))
            );
        }

        private static IO<Object, Failure, Connection> setAutoCommit(
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

        @Override
        public <T> IO<Object, Failure, T> transaction(
            Connection connection,
            IO<Object, Failure, T> dbCommand
        ) {
            return IO.bracket(
                setAutoCommit(connection, false),
                connection2 -> setAutoCommit(connection, true),
                connection3 -> dbCommand.foldM(
                    failure -> {
                        try {
                            connection3.rollback();
                        } catch(SQLException e) {};
                        return IO.fail(failure);
                    },
                    success -> {
                        try {
                            connection3.commit();
                        } catch(SQLException e) {};
                        return IO.succeed(success);
                    }
                )
            ).blocking();
        }
    }

    public static <T> IO<Environment, Failure, Connection> getConnection() {
        return IO.accessM(env -> env.get(Service.class).getConnection());
    }

    public static <T> IO<Environment, Failure, T> querySingle(
        Connection connection,
        String sql,
        Extractor<T> createObject,
        Object... params
    ) {
        return IO.accessM(env -> env.get(Service.class)
            .querySingle(connection, sql, createObject, params));
    }

    public static <T> IO<Environment, Failure, T> querySinglePrepared(
        Connection connection,
        String sql,
        ThrowingConsumer<PreparedStatement, SQLException> prepare,
        Extractor<T> createObject
    ) {
        return IO.accessM(env -> env.get(Service.class)
            .querySinglePrepared(connection, sql, prepare, createObject));
    }

    public static <R, T> IO<Environment, Failure, R> query(
        Connection connection,
        String sql,
        Extractor<T> createObject,
        Function<Iterator<T>, IO<Object, Failure, R>> fn,
        Object... params
    ) {
        return IO.accessM(env -> env.get(Service.class)
            .query(connection, sql, createObject, fn, params));
    }

    public static <R, T> IO<Environment, Failure, R> queryPrepared(
        Connection connection,
        String sql,
        ThrowingConsumer<PreparedStatement, SQLException> prepare,
        Extractor<T> createObject,
        Function<Iterator<T>, IO<Object, Failure, R>> fn
    ) {
        return IO.accessM(env -> env.get(Service.class)
            .queryPrepared(connection, sql, prepare, createObject, fn));
    }

    public static IO<Environment, Failure, Integer> update(
        Connection connection,
        final String sql
    ) {
        return IO.accessM(env -> env.get(Service.class)
            .update(connection, sql));
    }

    public static IO<Environment, Failure, Integer> updatePrepared(
        Connection connection,
        final String sql,
        final ThrowingConsumer<PreparedStatement, SQLException> prepare
    ) {
        return IO.accessM(env -> env.get(Service.class)
            .updatePrepared(connection, sql, prepare));
    }

    public static IO<Environment, Failure, Integer> batchUpdate(
        Connection connection,
        String... sqls
    ) {
        return IO.accessM(env -> env.get(Service.class)
            .batchUpdate(connection, sqls));
    }

    public static <T> IO<Environment, Failure, T> transaction(
        Connection connection,
        IO<Environment, Failure, T> dbCommand
    ) {
        return IO.accessM(env -> env.get(Service.class)
            .transaction(connection, dbCommand.provide(env)));
    }

    public static <T> IO<Object, Failure, Stream<T>> iterateToStream(
        Iterator<T> iterator
    ) {
        Builder<T> builder = Stream.builder();
        return iterateToStreamLoop(builder, iterator);
    }

    private static <T> IO<Object, Failure, Stream<T>> iterateToStreamLoop(
        Builder<T> builder,
        Iterator<T> iterator
    ) {
        return IO.<Object, Failure, Boolean>succeed(
            iterator.hasNext()
        ).flatMap(hasNext -> {
            if (hasNext) {
                T value = iterator.next();
                builder.accept(value);
                return iterateToStreamLoop(builder, iterator);
            } else {
                return IO.succeed(builder.build());
            }
        });
    }

    public static <T> IO<Object, Failure, List<T>> iterateToList(
        Iterator<T> iterator
    ) {
        List<T> list = new ArrayList<>();
        return iterateToListLoop(list, iterator);
    }

    private static <T> IO<Object, Failure, List<T>> iterateToListLoop(
        List<T> list,
        Iterator<T> iterator
    ) {
        return IO.<Object, Failure, Boolean>succeed(
            iterator.hasNext()
        ).flatMap(hasNext -> {
            if (hasNext) {
                T value = iterator.next();
                list.add(value);
                return iterateToListLoop(list, iterator);
            } else {
                return IO.succeed(list);
            }
        });
    }

    private static class ResultSetIterator<T>
        implements Iterator<T>, AutoCloseable
    {
        private final ResultSet resultSet;
        private final Extractor<T> extractor;

        public ResultSetIterator(final ResultSet resultSet, final Extractor<T> extractor) {
            this.extractor = extractor;
            this.resultSet = resultSet;
        }

        @Override
        public boolean hasNext() {
            try {
                return resultSet.next();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public T next() {
            try {
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
