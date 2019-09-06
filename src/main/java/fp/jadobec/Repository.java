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
    private final ThrowingSupplier<Connection, SQLException> connectionFactory;

    private Repository(final DataSource dataSource) {
        this.connectionFactory = () -> dataSource.getConnection();
    }

    public <T> Either<Failure, T> use(
    	Runtime<Void> runtime,
    	IO<Connection, Failure, T> command
    ) {
        try {
            final Connection connection = connectionFactory.get();
            final Either<Failure, T> result = runtime.unsafeRun(command.provide(connection));
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
        return queryPrepared(sql, prepare, createObject, Repository::getFirstFromIterator);
    }
    
    private static <T> IO<Connection, Failure, T> getFirstFromIterator(
    	Iterator<T> iterator
    ) {
    	if (iterator.hasNext()) {
    		return IO.succeed(iterator.next());
    	} else {
    		return IO.fail(GeneralFailure.of("Missing result")); 
    	}
    }

    public static <R, T> IO<Connection, Failure, R> query(
        String sql,
        Extractor<T> createObject,
        Function<Iterator<T>, IO<Connection, Failure, R>> fn,
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
        Function<Iterator<T>, IO<Connection, Failure, R>> fn
    ) {
        return IO.bracket(IO.absolve(IO.access((Connection connection) -> {
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

    public static IO<Connection, Failure, Integer> batchUpdate(String... sqls) {
    	return batchUpdateLoop(sqls, 0);
    }

    private static IO<Connection, Failure, Integer> batchUpdateLoop(String[] sqls, int index) {
    	return IO.succeed(sqls.length <= index).flatMap((Boolean b) -> b ?
			IO.<Connection, Failure, Integer>succeed((Integer) 0) :
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
                        return IO.fail(failure);
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
    
    public static <T> IO<Connection, Failure, Stream<T>> iterateToStream(
    	Iterator<T> iterator
    ) {
		Builder<T> builder = Stream.builder();
		return iterateToStreamLoop(builder, iterator);
    }
    
    private static <T> IO<Connection, Failure, Stream<T>> iterateToStreamLoop(
    	Builder<T> builder,
    	Iterator<T> iterator
    ) {
    	return IO.succeed(iterator.hasNext()).flatMap(hasNext -> {
    		if (hasNext) {
				T value = iterator.next();
				builder.accept(value);
				return iterateToStreamLoop(builder, iterator);
    		} else {
				return IO.succeed(builder.build());
    		}
    	});
    }
    
    public static <T> IO<Connection, Failure, List<T>> iterateToList(
    	Iterator<T> iterator
    ) {
		List<T> list = new ArrayList<>();
		return iterateToListLoop(list, iterator);
    }
    
    private static <T> IO<Connection, Failure, List<T>> iterateToListLoop(
    	List<T> list,
    	Iterator<T> iterator
    ) {
    	return IO.succeed(iterator.hasNext()).flatMap(hasNext -> {
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
