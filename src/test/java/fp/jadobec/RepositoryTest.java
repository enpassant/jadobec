package fp.jadobec;

import static org.junit.Assert.*;
import org.junit.Test;

import java.sql.Connection;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import java.util.stream.Collectors;
import java.util.function.Consumer;
import java.util.function.Supplier;

import fp.util.Either;
import fp.util.Failure;
import fp.util.Left;
import fp.util.Right;
import fp.util.Tuple2;

public class RepositoryTest {
    private final Person johnDoe = Person.of(1, "John Doe", 32);
    private final Person janeDoe = Person.of(2, "Jane Doe", 28);
    private final Person jakeDoe = Person.of(2, "Jake Doe", 28);
    private final Person jareDoe = Person.of(2, "Jare Doe", 28);
    private final Person jaredDoe = Person.of(null, "Jared Doe", 12);
    private final Person jaredDoeInserted = Person.of(3, "Jared Doe", 12);

    private final List<Person> expectedPersons = Arrays.asList(johnDoe, janeDoe);

    @Test
    public void testQuerySinglePerson() {
        checkDbCommand(
            Repository.querySingle(
                "SELECT id, name, age FROM person WHERE id = 2",
                rs -> Person.of(
                    rs.getInt("id"),
                    rs.getString("name"),
                    rs.getInt("age")
                )
            ).forEach(person ->
                assertEquals(janeDoe, person)
            )
        );
    }

    @Test
    public void testQueryPerson() {
        checkDbCommand(
            Repository.query(
                "SELECT id, name, age FROM person",
                rs -> Person.of(
                    rs.getInt("id"),
                    rs.getString("name"),
                    rs.getInt("age")
                )
            ).forEach(persons ->
                assertEquals(
                    expectedPersons,
                    persons
                )
            )
        );
    }

    @Test
    public void testQueryPreparedPerson() {
        checkDbCommand(
            Repository.queryPrepared(
                "SELECT id, name, age FROM person WHERE age < ?",
                ps -> ps.setInt(1, 40),
                rs -> Person.of(
                    rs.getInt("id"),
                    rs.getString("name"),
                    rs.getInt("age")
                )
            ).forEach(persons ->
                assertEquals(
                    expectedPersons,
                    persons
                )
            )
        );
    }

    @Test
    public void testUpdatePerson() {
        checkDbCommand(
            updatePersonName(2, "Jake Doe").then(
                selectSingleAsPerson(2)
            ).forEach(person ->
                assertEquals(jakeDoe, person)
            )
        );
    }

    @Test
    public void testUpdatePreparedPerson() {
        checkDbCommand(
            updatePersonName(2, "Jake Doe").then(
                selectSingleAsPerson(2)
            ).forEach(person ->
                assertEquals(jakeDoe, person)
            )
        );
    }

    @Test
    public void testGoodTransaction() {
        checkDbCommand(
            Repository.transaction(() ->
                updatePersonName(2, "Jake Doe").then(
                    updatePersonName(2, "Jare Doe")
            )).then(
                selectSingleAsPerson(2)
            ).forEach(person ->
                assertEquals(jareDoe, person)
            )
        );
    }

    @Test
    public void testBadTransaction() {
        checkDbCommand(
            Repository.transaction(() ->
                updatePersonName(2, "Jake Doe").then(
                    updatePersonName(2, null)
            )).recover(failure -> 1)
            .then(
                selectSingleAsPerson(2)
            ).forEach(person ->
                assertEquals(janeDoe, person)
            )
        );
    }

    private static DbCommand<Integer> updatePersonName( int id, String name) {
        return Repository.updatePrepared(
            "UPDATE person SET name=? WHERE id = ?",
            ps -> {
                ps.setString(1, name);
                ps.setInt(2, id);
            }
        );
    }

    private static DbCommand<Person> selectSingleAsPerson( Integer id) {
        return RepositoryMagic.querySingleAs(
            Person.class,
            "SELECT id, name, age FROM person p WHERE id = ?",
            id
        );
    }

    private static <T> void checkDbCommand(DbCommand<T> testDbCommand) {
        final Either<Failure, T> repositoryOrFailure = loadRepository()
            .flatMap(repository ->
                repository.use(
                    RepositoryTest.fill()
                        .flatMap(i -> testDbCommand)
                )
            );

        assertTrue(
            repositoryOrFailure.toString(),
            repositoryOrFailure.right().isPresent()
        );
    }

    private static Either<Failure, Repository> loadRepository() {
        return Repository.load(
            "org.h2.jdbcx.JdbcDataSource",
            "SELECT 1",
            Tuple2.of("URL", "jdbc:h2:mem:")
        );
    }

    private static DbCommand<Integer> fill() {
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
