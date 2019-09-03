package fp.jadobec;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Test;

import fp.io.IO;
import fp.util.Either;
import fp.util.Failure;
import fp.util.Tuple2;

public class RepositoryTest {
    private final Person johnDoe = Person.of(1, "John Doe", 32);
    private final Person janeDoe = Person.of(2, "Jane Doe", 28);
    private final Person jakeDoe = Person.of(2, "Jake Doe", 28);
    private final Person jareDoe = Person.of(2, "Jare Doe", 28);

    private final List<Person> expectedPersons = Arrays.asList(johnDoe, janeDoe);

    @Test
    public void testQuerySinglePersonIO() {
        checkDbCommandIO(
            Repository.querySingle(
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
    public void testQueryPersonIO() {
        checkDbCommandIO(
            Repository.query(
                "SELECT id, name, age FROM person",
                rs -> Person.of(
                    rs.getInt("id"),
                    rs.getString("name"),
                    rs.getInt("age")
                )
            ).peek(persons ->
                assertEquals(
                    expectedPersons,
                    persons.collect(Collectors.toList())
                )
            )
        );
    }

    @Test
    public void testQueryPreparedPersonIO() {
        checkDbCommandIO(
            Repository.queryPrepared(
                "SELECT id, name, age FROM person WHERE age < ?",
                ps -> ps.setInt(1, 40),
                rs -> Person.of(
                    rs.getInt("id"),
                    rs.getString("name"),
                    rs.getInt("age")
                )
            ).peek(persons ->
                assertEquals(
                    expectedPersons,
                    persons.collect(Collectors.toList())
                )
            )
        );
    }

    @Test
    public void testUpdatePersonIO() {
        checkDbCommandIO(
            updatePersonNameIO(2, "Jake Doe").flatMap(v ->
                selectSingleAsPersonIO(2)
            ).peek(person ->
                assertEquals(jakeDoe, person)
            )
        );
    }

    @Test
    public void testUpdatePreparedPersonIO() {
        checkDbCommandIO(
            updatePersonNameIO(2, "Jake Doe").flatMap(v ->
                selectSingleAsPersonIO(2)
            ).peek(person ->
                assertEquals(jakeDoe, person)
            )
        );
    }

    @Test
    public void testGoodTransaction() {
        checkDbCommandIO(
            Repository.transaction(
                updatePersonNameIO(2, "Jake Doe").flatMap(v ->
                    updatePersonNameIO(2, "Jare Doe")
            )).flatMap(v ->
                selectSingleAsPersonIO(2)
            ).peek(person ->
                assertEquals(jareDoe, person)
            )
        );
    }

    @Test
    public void testBadTransaction() {
        checkDbCommandIO(
            Repository.transaction(
                updatePersonNameIO(2, "Jake Doe").flatMap(v ->
                    updatePersonNameIO(2, null)
            )).foldM(failure -> IO.succeed(1), success -> IO.succeed(success))
            .flatMap(v ->
                selectSingleAsPersonIO(2)
            ).peek(person ->
                assertEquals(janeDoe, person)
            )
        );
    }

    private static IO<Connection, Failure, Integer> updatePersonNameIO(int id, String name) {
        return Repository.updatePrepared(
            "UPDATE person SET name=? WHERE id = ?",
            ps -> {
                ps.setString(1, name);
                ps.setInt(2, id);
            }
        );
    }

    private static IO<Connection, Failure, Person> selectSingleAsPersonIO( Integer id) {
        return RepositoryMagic.querySingleAs(
            Person.class,
            "SELECT id, name, age FROM person p WHERE id = ?",
            id
        );
    }

    private static <T> void checkDbCommandIO(IO<Connection, Failure, T> testDbCommand) {
        final Either<Failure, T> repositoryOrFailure = createRepository()
            .flatMap(repository ->
                repository.use(
                    RepositoryTest.fillIO()
                        .flatMap(i -> testDbCommand)
                )
            );

        assertTrue(
            repositoryOrFailure.toString(),
            repositoryOrFailure.right().isPresent()
        );
    }

    private static Either<Failure, Repository> createRepository() {
        return Repository.create(
            "org.h2.jdbcx.JdbcDataSource",
            "SELECT 1",
            Tuple2.of("URL", "jdbc:h2:mem:")
        );
    }

    private static IO<Connection, Failure, Integer> fillIO() {
        return Repository.batchUpdate(
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
