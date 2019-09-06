package fp.jadobec;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import fp.io.IO;
import fp.util.Either;
import fp.util.ExceptionFailure;
import fp.util.Failure;
import fp.util.Left;
import fp.util.Right;
import fp.util.ThrowingConsumer;

public class RepositoryMagic {
    public static <T> IO<Connection, Failure, T> querySingleAs(
        Class<T> type,
        String sql,
        Object... params
    ) {
		ThrowingConsumer<PreparedStatement, SQLException> prepare = ps -> {
			for (int i=0; i<params.length; i++) {
				ps.setObject(i + 1, params[i]);
			}
		};

		return Repository.querySinglePrepared(
			sql,
			prepare,
			Record.expandAs(type)
		).flatMap(either -> IO.absolve(IO.succeed(either)));
    }

    public static <T> IO<Connection, Failure, List<T>> queryAs(
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

    @SuppressWarnings("unchecked")
	public static <T> IO<Connection, Failure, List<T>> queryPreparedAs(
        Class<T> type,
        String sql,
        ThrowingConsumer<PreparedStatement, SQLException> prepare
    ) {
        return IO.absolve(IO.access(connection -> {
            PreparedStatement stmt = null;

            try {
                stmt = connection.prepareStatement(sql);

                prepare.accept(stmt);

                ResultSet rs = stmt.executeQuery();

                List<T> list = new ArrayList<T>();
                while(rs.next()) {
                    Either<Failure, T> createdObjectOrFailure =
                        Record.expandAs(type).extract(rs);
                    if (createdObjectOrFailure.isLeft()) {
                        rs.close();
                        return (Either<Failure, List<T>>) createdObjectOrFailure;
                    }
                    list.add(createdObjectOrFailure.right());
                }
                rs.close();

                return Right.of(list);
            } catch (Exception e) {
                return Left.of(
                	ExceptionFailure.of(e)
                );
            } finally {
                try {
                    if (stmt != null)
                        stmt.close();
                } catch (SQLException e) {
                }
            }
        }));
    }

    public static IO<Connection, Failure, Integer> insert(Object object) {
        return Record.from(object).fold(
        	failure -> IO.fail(failure),
        	record -> {
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
	
	            return Repository.updatePrepared(sql, prepare);
        	}
        );
    }
}
