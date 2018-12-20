package fp.jadobec;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.sql.ConnectionPoolDataSource;
import javax.sql.DataSource;

import fp.util.Either;
import fp.util.Failure;
import fp.util.Left;
import fp.util.Right;
import fp.util.Tuple2;

public class Repository {
    private final ThrowingSupplier<Connection, SQLException> connectionFactory;

    private Repository(final DataSource dataSource) {
        this.connectionFactory = () -> dataSource.getConnection();
    }

    public <T> Either<Failure, T> use(DbCommand<T> command) {
        try {
            final Connection connection = connectionFactory.get();
            final Either<Failure, T> result = command.apply(connection);
            connection.close();
            return result;
        } catch (SQLException e) {
            return Left.of(Failure.of(
                e.getClass().getSimpleName(),
                Failure.EXCEPTION,
                e
            ));
        }
    }

    public static Either<Failure, Repository> load(
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
                Failure.of(e.getClass().getSimpleName(), Failure.EXCEPTION, e)
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

    public static Either<Failure, Repository> load(
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
                Failure.of(e.getClass().getSimpleName(), Failure.EXCEPTION, e)
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

    public static <T> DbCommand<T> querySingle(
        String sql,
        ThrowingFunction<ResultSet, T, SQLException> createObject,
        Object... params
    ) {
        ThrowingConsumer<PreparedStatement, SQLException> prepare = ps -> {
            for (int i=0; i<params.length; i++) {
                ps.setObject(i + 1, params[i]);
            }
        };

        return querySinglePrepared(
            sql,
            prepare,
            createObject
        );
    }

    public static <T> DbCommand<T> querySingleAs(
        Class<T> type,
        String sql,
        Object... params
    ) {
        return connection -> {
            ThrowingConsumer<PreparedStatement, SQLException> prepare = ps -> {
                for (int i=0; i<params.length; i++) {
                    ps.setObject(i + 1, params[i]);
                }
            };

            return querySinglePrepared(
                sql,
                prepare,
                Record.expandAs(type)
            ).apply(connection).flatten();
        };
    }

    public static <T> DbCommand<T> querySinglePreparedAs(
        Class<T> type,
        String sql,
        ThrowingConsumer<PreparedStatement, SQLException> prepare
    ) {
        return querySinglePrepared(sql, prepare, Record.expandAs(type))
            .flatten();
    }

    public static <T> DbCommand<T> querySinglePrepared(
        String sql,
        ThrowingConsumer<PreparedStatement, SQLException> prepare,
        ThrowingFunction<ResultSet, T, SQLException> createObject
    ) {
        return connection -> {
            PreparedStatement stmt = null;

            try {
                stmt = connection.prepareStatement(sql);

                prepare.accept(stmt);

                ResultSet rs = stmt.executeQuery();

                T createdObject = null;
                while (rs.next()) {
                    createdObject = createObject.apply(rs);
                    if (createdObject != null) {
                        break;
                    }
                }
                rs.close();
                return (createdObject == null) ?
                    Left.of(Failure.of("SqlQueryFailed")) :
                    Right.of(createdObject);
            } catch (Exception e) {
                return Left.of(
                    Failure.of(e.getClass().getSimpleName(), Failure.EXCEPTION, e)
                );
            } finally {
                try {
                    if (stmt != null)
                        stmt.close();
                } catch (SQLException e) {
                }
            }
        };
    }

    public static <T> DbCommand<List<T>> queryAs(
        Class<T> type,
        String sql,
        Object... params
    ) {
        ThrowingConsumer<PreparedStatement, SQLException> prepare = ps -> {
            for (int i=0; i<params.length; i++) {
                ps.setObject(i + 1, params[i]);
            }
        };

        return queryPreparedAs(type, sql, prepare);
    }

    public static <T> DbCommand<List<T>> query(
        String sql,
        ThrowingFunction<ResultSet, T, SQLException> createObject,
        Object... params
    ) {
        ThrowingConsumer<PreparedStatement, SQLException> prepare = ps -> {
            for (int i=0; i<params.length; i++) {
                ps.setObject(i + 1, params[i]);
            }
        };

        return queryPrepared(
            sql,
            prepare,
            createObject
        );
    }

    public static <T> DbCommand<List<T>> queryPreparedAs(
        Class<T> type,
        String sql,
        ThrowingConsumer<PreparedStatement, SQLException> prepare
    ) {
        return connection -> {
            PreparedStatement stmt = null;

            try {
                stmt = connection.prepareStatement(sql);

                prepare.accept(stmt);

                ResultSet rs = stmt.executeQuery();

                List<T> list = new ArrayList();
                while(rs.next()) {
                    Either<Failure, T> createdObjectOrFailure =
                        Record.expandAs(type).apply(rs);
                    if (createdObjectOrFailure.left().isPresent()) {
                        rs.close();
                        return (Either<Failure, List<T>>) createdObjectOrFailure;
                    }
                    list.add(createdObjectOrFailure.right().get());
                }
                rs.close();

                return Right.of(list);
            } catch (Exception e) {
                return Left.of(
                    Failure.of(e.getClass().getSimpleName(), Failure.EXCEPTION, e)
                );
            } finally {
                try {
                    if (stmt != null)
                        stmt.close();
                } catch (SQLException e) {
                }
            }
        };
    }

    public static <T> DbCommand<List<T>> queryPrepared(
        String sql,
        ThrowingConsumer<PreparedStatement, SQLException> prepare,
        ThrowingFunction<ResultSet, T, SQLException> createObject
    ) {
        return connection -> {
            PreparedStatement stmt = null;

            try {
                stmt = connection.prepareStatement(sql);

                prepare.accept(stmt);

                ResultSet rs = stmt.executeQuery();

                List<T> list = new ArrayList();
                while(rs.next()) {
                    T createdObject = createObject.apply(rs);
                    list.add(createdObject);
                }
                rs.close();

                return Right.of(list);
            } catch (Exception e) {
                return Left.of(
                    Failure.of(e.getClass().getSimpleName(), Failure.EXCEPTION, e)
                );
            } finally {
                try {
                    if (stmt != null)
                        stmt.close();
                } catch (SQLException e) {
                }
            }
        };
    }

    public static DbCommand<Integer> update(final String sql) {
        return updatePrepared(sql, ps -> {});
    }

    public static DbCommand<Integer> updatePrepared(
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
                    Failure.of(e.getClass().getSimpleName(), Failure.EXCEPTION, e)
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

    public static DbCommand<Integer> batchUpdate(String... sqls) {
        return connection -> {
            return Stream.of(sqls)
                .collect(Collectors.reducing(
                    Right.of(0),
                    sql -> Repository.update(sql).apply(connection),
                    (s, v) -> s.flatMap(i -> v)
                ));
        };
    }

    public static DbCommand<Integer> insert(Object object) {
        return connection ->
            Record.from(object).flatMap(record -> {
                final String fields = record
                    .fields()
                    .stream()
                    .collect(Collectors.joining(", "));
                final String values = record
                    .fields()
                    .stream()
                    .map(f -> "?")
                    .collect(Collectors.joining(", "));
                final Object[] params = record.values().toArray();
                final String sql =
                    "insert into " +
                    object.getClass().getSimpleName() +
                    "(" +
                    fields +
                    ") values(" +
                    values +
                    ")"
                ;
                final ThrowingConsumer<PreparedStatement, SQLException> prepare =
                ps -> {
                    for (int i=0; i<params.length; i++) {
                        ps.setObject(i + 1, params[i]);
                    }
                };

                return updatePrepared(sql, prepare).apply(connection);
            });
    }

    public static <T> DbCommand<T> transaction(
        Supplier<DbCommand<T>> supplier
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
                    Failure.of(e.getClass().getSimpleName(), Failure.EXCEPTION, e)
                );
            } finally {
                try {
                    connection.setAutoCommit(true);
                } catch (SQLException e) {
                }
            }
        };
    }
}
