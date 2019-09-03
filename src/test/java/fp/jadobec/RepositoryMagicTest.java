package fp.jadobec;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import fp.io.IO;
import fp.util.Either;
import fp.util.Failure;
import fp.util.GeneralFailure;
import fp.util.Tuple2;

public class RepositoryMagicTest {
    private final Person johnDoe = Person.of(1, "John Doe", 32);
    private final Person janeDoe = Person.of(2, "Jane Doe", 28);
    private final Person jaredDoe = Person.of(null, "Jared Doe", 12);
    private final Person jaredDoeInserted = Person.of(3, "Jared Doe", 12);

    private final List<Person> expectedPersons = Arrays.asList(johnDoe, janeDoe);

    @Test
    public void testQuerySingleAsPerson() {
        checkDbCommand(
            RepositoryMagic.querySingleAs(
                Person.class,
                "SELECT id, name, age FROM person p WHERE id = ? and age < ?",
                2,
                30
            ).peek(person->
                assertEquals(janeDoe, person)
            )
        );
    }

    @Test
    public void testQueryAsPerson() {
        checkDbCommand(
            RepositoryMagic.queryAs(
                Person.class,
                "SELECT id, name, age FROM person"
            ).peek(persons ->
                assertEquals(
                    expectedPersons,
                    persons
                )
            )
        );
    }

    @Test
    public void testQueryAsPersonFailed() {
        checkDbCommand(
            RepositoryMagic.queryAs(
                Person.class,
                "SELECT id, name FROM person"
            ).flatMap(person ->
                IO.fail((Failure) GeneralFailure.of("Wrong result!"))
            ).foldM(failure -> {
                assertEquals(
                    "ExceptionFailure(java.lang.IllegalArgumentException: " +
                        "wrong number of arguments)",
                    failure.toString()
                );
                return IO.succeed(1);
            }, success -> IO.succeed(success))
        );
    }

    @Test
    public void testQueryPreparedAsPerson() {
        checkDbCommand(
            RepositoryMagic.queryPreparedAs(
                Person.class,
                "SELECT id, name, age FROM person WHERE age < ?",
                ps -> ps.setInt(1, 40)
            ).peek(persons ->
                assertEquals(
                    expectedPersons,
                    persons
                )
            )
        );
    }

    @Test
    public void testInsertPerson() {
        checkDbCommand(
            RepositoryMagic.insert(jaredDoe).flatMap(v ->
                selectSingleAsPerson(3)
            ).peek(person ->
                assertEquals(jaredDoeInserted, person)
            )
        );
    }

    private static IO<Connection, Failure, Person> selectSingleAsPerson( Integer id) {
        return RepositoryMagic.querySingleAs(
            Person.class,
            "SELECT id, name, age FROM person p WHERE id = ?",
            id
        );
    }

    private static <T> void checkDbCommand(IO<Connection, Failure, T> testDbCommand) {
        final Either<Failure, T> repositoryOrFailure = createRepository()
            .flatMap(repository ->
                repository.use(
                    RepositoryMagicTest.fill()
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

    private static IO<Connection, Failure, Integer> fill() {
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
