package fp.jadobec;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.Optional;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.sql.DataSource;

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
    private final ThrowingSupplier<Connection, SQLException> connectionFactory;

    private Repository(final DataSource dataSource) {
        this.connectionFactory = () -> dataSource.getConnection();
    }

    public <T> Either<Failure, T> use(IO<Connection, Failure, T> command) {
        try {
            final Connection connection = connectionFactory.get();
            final Either<Failure, T> result = IO.evaluate(connection, command);
            connection.close();
            return result;
        } catch (SQLException e) {
            return Left.of(ExceptionFailure.of(e));
        }
    }

    public static Either<Failure, Repository> create(
        DataSource dataSource,
        String testSql
    ) {
        return ExceptionFailure.tryCatchFinal(
            () -> dataSource.getConnection().createStatement(),
            stmt -> {
                ResultSet rs = stmt.executeQuery(testSql);
                rs.close();
                return new Repository(dataSource);
            },
            stmt -> stmt.close()
        );
    }

    @SafeVarargs
	public static Either<Failure, Repository> create(
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
            return Right.of(new Repository(dataSource));
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
        return queryPrepared(sql, prepare, createObject)
            .flatMap(items -> {
            	final Optional<T> firstValue = items.findFirst();
            	items.close();
                return firstValue.isPresent() ?
                    IO.pure(firstValue.get()) :
                    IO.fail((Failure) GeneralFailure.of("Missing result"))
                ;
            });
    }

    public static <T> IO<Connection, Failure, Stream<T>> query(
        String sql,
        Extractor<T> createObject,
        Object... params
    ) {
        ThrowingConsumer<PreparedStatement, SQLException> prepare = ps -> {
            for (int i=0; i<params.length; i++) {
                ps.setObject(i + 1, params[i]);
            }
        };

        return queryPrepared(sql, prepare, createObject);
    }

    public static <T> IO<Connection, Failure, Stream<T>> queryPrepared(
        String sql,
        ThrowingConsumer<PreparedStatement, SQLException> prepare,
        Extractor<T> createObject
    ) {
        return IO.absolve(IO.access((Connection connection) -> {
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
        }));
    }

    public static IO<Connection, Failure, Integer> update(final String sql) {
        return updatePrepared(sql, ps -> {});
    }

    public static IO<Connection, Failure, Integer> updatePrepared(
        final String sql,
        final ThrowingConsumer<PreparedStatement, SQLException> prepare
    ) {
        return IO.absolve(IO.access(connection -> {
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
                	ExceptionFailure.of(e)
                );
            } finally {
                try {
                    if (stmt != null) stmt.close();
                } catch (SQLException e) {
                    //logger.error("Update prepared close error", e);
                }
            }
        }));
    }

    public static IO<Connection, Failure, Integer> batchUpdate(String... sqls) {
        return IO.absolve(IO.access(connection -> {
            return Stream.of(sqls)
                .collect(Collectors.reducing(
                    Right.of(0),
                    sql -> IO.evaluate(connection, Repository.update(sql)),
                    (s, v) -> s.flatMap(i -> v)
                ));
        }));
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
                connection2 -> setAutoCommit(connection2, true),
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
                        return IO.pure(success);
                    }
                )
            )
        );
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
