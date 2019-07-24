package fp.jadobec;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import fp.util.Either;
import fp.util.Failure;
import fp.util.GeneralFailure;
import fp.util.Left;
import fp.util.Right;

public class RepositoryMagic {
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

            return Repository.querySinglePrepared(
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
        return Repository.querySinglePrepared(sql, prepare, Record.expandAs(type))
            .flatten();
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
                        Record.expandAs(type).extract(rs);
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
                	GeneralFailure.of(e)
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

                return Repository.updatePrepared(sql, prepare).apply(connection);
            });
    }
}
