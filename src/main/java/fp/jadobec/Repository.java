package jadobec;

import java.lang.reflect.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.sql.DataSource;

import util.Either;
import util.Failure;
import util.Left;
import util.Right;
import util.Tuple2;

public class Repository implements AutoCloseable {
    private final DataSource dataSource;
    private Connection conn;

    private Repository(final DataSource dataSource, Connection conn) {
        this.dataSource = dataSource;
        this.conn = conn;
    }

    public void close() {
        try {
            if (conn != null)
                conn.close();
        } catch (SQLException e) {
        }
    }

    public void openConnection() {
        try {
            if (conn.isClosed()) {
                conn = dataSource.getConnection();
            }
        } catch (SQLException e) {
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
            final DataSource dataSource = (DataSource) type.newInstance();
            for (final Tuple2<String, String> property : properties) {
                final Method method =
                    type.getDeclaredMethod("set" + property.getFirst(), String.class);
                method.invoke(dataSource, property.getSecond());
            }

            conn = dataSource.getConnection();
            stmt = conn.createStatement();

            ResultSet rs = stmt.executeQuery(testSql);
            rs.close();
            return Right.of(new Repository(dataSource, conn));
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

    public <T> Either<Failure, T> querySingle(
        String sql,
        ThrowingFunction<ResultSet, T, SQLException> createObject
    ) {
        ThrowingConsumer<PreparedStatement, SQLException> prepare = ps -> {};

        return querySinglePrepared(
            sql,
            prepare,
            createObject
        );
    }

    public <T> Either<Failure, T> querySingleAs(
        Class<T> type,
        String sql,
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
            Record.expandAs(type)
        ).flatten();
    }

    public <T> Either<Failure, T> querySinglePreparedAs(
        Class<T> type,
        String sql,
        ThrowingConsumer<PreparedStatement, SQLException> prepare
    ) {
        return querySinglePrepared(sql, prepare, Record.expandAs(type)).flatten();
    }

    public <T> Either<Failure, T> querySinglePrepared(
        String sql,
        ThrowingConsumer<PreparedStatement, SQLException> prepare,
        ThrowingFunction<ResultSet, T, SQLException> createObject
    ) {
        PreparedStatement stmt = null;

        try {
            stmt = conn.prepareStatement(sql);

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
    }

    public <T> Either<Failure, Stream<T>> queryAs(
        Class<T> type,
        String sql
    ) {
        ThrowingConsumer<PreparedStatement, SQLException> prepare = ps -> {};

        return queryPreparedAs(type, sql, prepare);
    }

    public <T> Either<Failure, Stream<T>> query(
        String sql,
        ThrowingFunction<ResultSet, T, SQLException> createObject
    ) {
        ThrowingConsumer<PreparedStatement, SQLException> prepare = ps -> {};

        return queryPrepared(
            sql,
            prepare,
            createObject
        );
    }

    public <T> Either<Failure, Stream<T>> queryPreparedAs(
        Class<T> type,
        String sql,
        ThrowingConsumer<PreparedStatement, SQLException> prepare
    ) {
        PreparedStatement stmt = null;

        try {
            stmt = conn.prepareStatement(sql);

            prepare.accept(stmt);

            ResultSet rs = stmt.executeQuery();

            Stream.Builder<T> builder = Stream.builder();
            while(rs.next()) {
                Either<Failure, T> createdObjectOrFailure =
                    Record.expandAs(type).apply(rs);
                if (createdObjectOrFailure.left().isPresent()) {
                    rs.close();
                    return (Either<Failure, Stream<T>>) createdObjectOrFailure;
                }
                builder.accept(createdObjectOrFailure.right().get());
            }
            rs.close();

            return Right.of(builder.build());
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
    }

    public <T> Either<Failure, Stream<T>> queryPrepared(
        String sql,
        ThrowingConsumer<PreparedStatement, SQLException> prepare,
        ThrowingFunction<ResultSet, T, SQLException> createObject
    ) {
        PreparedStatement stmt = null;

        try {
            stmt = conn.prepareStatement(sql);

            prepare.accept(stmt);

            ResultSet rs = stmt.executeQuery();

            Stream.Builder<T> builder = Stream.builder();
            while(rs.next()) {
                T createdObject = createObject.apply(rs);
                builder.accept(createdObject);
            }
            rs.close();

            return Right.of(builder.build());
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
    }

    public Either<Failure, Integer> update(String sql) {
        return updatePrepared(sql, ps -> {});
    }

    public Either<Failure, Integer> updatePrepared(
        String sql,
        ThrowingConsumer<PreparedStatement, SQLException> prepare
    ) {
        PreparedStatement stmt = null;

        try {
            stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);

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
    }

    public Either<Failure, Integer> batchUpdate(String... sqls) {
        Either<Failure, Integer> init = Right.of(0);
        return Stream.of(sqls)
            .collect(Collectors.reducing(
                init,
                this::update,
                (s, v) -> s.flatMap(i -> v)
            ));
    }

    public Either<Failure, Integer> insert(Object object) {
        return Record.from(object).flatMap(record -> {
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

            return updatePrepared(sql, prepare);
        });
    }

    public <T> Either<Failure, T> runInTransaction(
        Supplier<Either<Failure, T>> supplier
    ) {
        try {
            conn.setAutoCommit(false);
            Either<Failure, T> result = supplier.get();

            if (result.right().isPresent()) {
                conn.commit();
            } else {
                conn.rollback();
            }

            return result;
        } catch (SQLException e) {
            try {
                conn.rollback();
            } catch (SQLException e1) {
            }
            return Left.of(
                Failure.of(e.getClass().getSimpleName(), Failure.EXCEPTION, e)
            );
        } finally {
            try {
                conn.setAutoCommit(true);
            } catch (SQLException e) {
            }
        }
    }
}
