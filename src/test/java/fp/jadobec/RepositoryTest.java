package fp.jadobec;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.util.Arrays;
import java.util.function.Function;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Test;

import fp.io.DefaultPlatform;
import fp.io.DefaultRuntime;
import fp.io.Environment;
import fp.io.IO;
import fp.io.Runtime;
import fp.util.Either;
import fp.util.Failure;
import fp.util.Tuple2;

public class RepositoryTest {
    final static DefaultPlatform platform = new DefaultPlatform();

    final static Runtime<Void> defaultRuntime = new DefaultRuntime<Void>(null, platform);

    @AfterClass
    public static void setUp() {
        platform.shutdown();
    }

    private final Person johnDoe = Person.of(1, "John Doe", 32);
    private final Person janeDoe = Person.of(2, "Jane Doe", 28);
    private final Person jakeDoe = Person.of(2, "Jake Doe", 28);
    private final Person jareDoe = Person.of(2, "Jare Doe", 28);

    private final List<Person> expectedPersons = Arrays.asList(johnDoe, janeDoe);

    @Test
    public void testQuerySinglePerson() {
        checkDbCommand(connection ->
            Repository.querySingle(
                connection,
                "SELECT id, name, age FROM person WHERE id = 2",
                rs -> Person.of(
                    rs.getInt("id"),
                    rs.getString("name"),
                    rs.getInt("age")
                )
            ).peek(person ->
                assertEquals(janeDoe, person)
            )
        );
    }

    @Test
    public void testQueryPerson() {
        checkDbCommand(connection ->
            Repository.query(
                connection,
                "SELECT id, name, age FROM person",
                rs -> Person.of(
                    rs.getInt("id"),
                    rs.getString("name"),
                    rs.getInt("age")
                ),
                Repository::iterateToList
            ).peek(persons ->
                assertEquals(expectedPersons, persons)
            )
        );
    }

    @Test
    public void testQueryPreparedPerson() {
        checkDbCommand(connection ->
            Repository.queryPrepared(
                connection,
                "SELECT id, name, age FROM person WHERE age < ?",
                ps -> ps.setInt(1, 40),
                rs -> Person.of(
                    rs.getInt("id"),
                    rs.getString("name"),
                    rs.getInt("age")
                ),
                Repository::iterateToList
            ).peek(persons ->
                assertEquals(expectedPersons, persons)
            )
        );
    }

    @Test
    public void testUpdatePerson() {
        checkDbCommand(connection ->
            updatePersonName(connection, 2, "Jake Doe").flatMap(v ->
                selectSingleAsPerson(connection, 2)
            ).peek(person ->
                assertEquals(jakeDoe, person)
            )
        );
    }

    @Test
    public void testUpdatePreparedPerson() {
        checkDbCommand(connection ->
            updatePersonName(connection, 2, "Jake Doe").flatMap(v ->
                selectSingleAsPerson(connection, 2)
            ).peek(person ->
                assertEquals(jakeDoe, person)
            )
        );
    }

    @Test
    public void testGoodTransaction() {
        checkDbCommand(connection ->
            Repository.transaction(
                connection,
                updatePersonName(connection, 2, "Jake Doe").flatMap(v ->
                    updatePersonName(connection, 2, "Jare Doe")
            )).flatMap(v ->
                selectSingleAsPerson(connection, 2)
            ).peek(person ->
                assertEquals(jareDoe, person)
            )
        );
    }

    @Test
    public void testBadTransaction() {
        checkDbCommand(connection ->
            Repository.transaction(
                connection,
                updatePersonName(connection, 2, "Jake Doe").flatMap(v ->
                    updatePersonName(connection, 2, null)
            )).foldM(failure -> IO.succeed(1), success -> IO.succeed(success))
            .flatMap(v ->
                selectSingleAsPerson(connection, 2)
            ).peek(person ->
                assertEquals(janeDoe, person)
            )
        );
    }

    private static IO<Environment, Failure, Integer> updatePersonName(
        Connection connection,
        int id,
        String name
    ) {
        return Repository.updatePrepared(
            connection,
            "UPDATE person SET name=? WHERE id = ?",
            ps -> {
                ps.setString(1, name);
                ps.setInt(2, id);
            }
        );
    }

    private static IO<Environment, Failure, Person> selectSingleAsPerson(
        Connection connection,
        Integer id
    ) {
        return Repository.querySingle(
            connection,
            "SELECT id, name, age FROM person p WHERE id = ?",
            rs -> Person.of(
                rs.getInt("id"),
                rs.getString("name"),
                rs.getInt("age")
            ),
            id
        );
    }

    private static <T> void checkDbCommand(
        Function<Connection, IO<Environment, Failure, T>> testDbCommand
    ) {
        final Either<Failure, T> repositoryOrFailure = createRepository()
            .flatMap(repository -> {
                final Environment environment =
                    Environment.of(Repository.Service.class, repository);
                return defaultRuntime.unsafeRun(
                    IO.bracket(
                        Repository.getConnection(),
                        connection -> IO.effect(() -> connection.close()),
                        connection ->
                            RepositoryTest.fill(connection)
                                .flatMap(i -> testDbCommand.apply(connection))
                ).provide(environment));
            });

        assertTrue(
            repositoryOrFailure.toString(),
            repositoryOrFailure.isRight()
        );
    }

    private static Either<Failure, Repository.Live> createRepository() {
        return Repository.Live.create(
            "org.h2.jdbcx.JdbcDataSource",
            "SELECT 1",
            Tuple2.of("URL", "jdbc:h2:mem:")
        );
    }

    private static IO<Environment, Failure, Integer> fill(Connection connection) {
        return Repository.batchUpdate(
            connection,
            "CREATE TABLE person(" +
                "id INT auto_increment, " +
                "name VARCHAR(30) NOT NULL, " +
                "age INT" +
            ")",
            "INSERT INTO person VALUES(1, 'John Doe', 32)",
            "INSERT INTO person VALUES(2, 'Jane Doe', 28)"
        );
    }
}
