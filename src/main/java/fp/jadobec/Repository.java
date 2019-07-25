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
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.sql.DataSource;

import fp.util.Either;
import fp.util.Failure;
import fp.util.GeneralFailure;
import fp.util.Left;
import fp.util.Right;
import fp.util.Tuple2;

public class Repository {
    private final ThrowingSupplier<Connection, SQLException> connectionFactory;

    private Repository(final DataSource dataSource) {
        this.connectionFactory = () -> dataSource.getConnection();
    }

    public <T> Either<Failure, T> use(DbCommand<Failure, T> command) {
        try {
            final Connection connection = connectionFactory.get();
            final Either<Failure, T> result = command.apply(connection);
            connection.close();
            return result;
        } catch (SQLException e) {
            return Left.of(GeneralFailure.of(e));
        }
    }

    public static Either<Failure, Repository> create(
        DataSource dataSource,
        String testSql
    ) {
        Connection conn = null;
        Statement stmt = null;

        try {
            conn = dataSource.getConnection();
            stmt = conn.createStatement();

            ResultSet rs = stmt.executeQuery(testSql);
            rs.close();
            return Right.of(new Repository(dataSource));
        } catch (Exception e) {
            return Left.of(
            	GeneralFailure.of(e)
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
            	GeneralFailure.of(e)
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

    public static <T> DbCommand<Failure, T> querySingle(
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

    public static <T> DbCommand<Failure, T> querySinglePrepared(
        String sql,
        ThrowingConsumer<PreparedStatement, SQLException> prepare,
        Extractor<T> createObject
    ) {
        return queryPrepared(sql, prepare, createObject)
            .flatMap(items -> connection -> {
            	final Optional<T> firstValue = items.findFirst();
            	items.close();
                return Either.ofOptional(
                	GeneralFailure.of("Missing result"),
                    firstValue
                );
            });
    }

    public static <T> DbCommand<Failure, Stream<T>> query(
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

    public static <T> DbCommand<Failure, Stream<T>> queryPrepared(
        String sql,
        ThrowingConsumer<PreparedStatement, SQLException> prepare,
        Extractor<T> createObject
    ) {
        return connection -> {
            PreparedStatement stmt = null;

            try {
                stmt = connection.prepareStatement(sql);

                prepare.accept(stmt);

                ResultSet rs = stmt.executeQuery();
                return Right.of(stream(rs, createObject));
            } catch (Exception e) {
                return Left.of(
                	GeneralFailure.of(e)
                );
            }
        };
    }

    public static DbCommand<Failure, Integer> update(final String sql) {
        return updatePrepared(sql, ps -> {});
    }

    public static DbCommand<Failure, Integer> updatePrepared(
        final String sql,
        final ThrowingConsumer<PreparedStatement, SQLException> prepare
    ) {
        return connection -> {
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
                	GeneralFailure.of(e)
                );
            } finally {
                try {
                    if (stmt != null) stmt.close();
                } catch (SQLException e) {
                    //logger.error("Update prepared close error", e);
                }
            }
        };
    }

    public static DbCommand<Failure, Integer> batchUpdate(String... sqls) {
        return connection -> {
            return Stream.of(sqls)
                .collect(Collectors.reducing(
                    Right.of(0),
                    sql -> Repository.update(sql).apply(connection),
                    (s, v) -> s.flatMap(i -> v)
                ));
        };
    }

    public static <T> DbCommand<Failure, T> transaction(
        Supplier<DbCommand<Failure, T>> supplier
    ) {
        return connection -> {
            try {
                connection.setAutoCommit(false);
                Either<Failure, T> result = supplier.get().apply(connection);

                if (result.right().isPresent()) {
                    connection.commit();
                } else {
                    connection.rollback();
                }

                return result;
            } catch (SQLException e) {
                try {
                    connection.rollback();
                } catch (SQLException e1) {
                }
                return Left.of(
                	GeneralFailure.of(e)
                );
            } finally {
                try {
                    connection.setAutoCommit(true);
                } catch (SQLException e) {
                }
            }
        };
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
