package fp.jadobec;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import fp.util.Either;
import fp.util.Failure;
import fp.util.GeneralFailure;
import fp.util.Left;
import fp.util.Tuple2;

public class RepositoryMagicTest {
    private final Person johnDoe = Person.of(1, "John Doe", 32);
    private final Person janeDoe = Person.of(2, "Jane Doe", 28);
    private final Person jakeDoe = Person.of(2, "Jake Doe", 28);
    private final Person jareDoe = Person.of(2, "Jare Doe", 28);
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
            ).forEach(person->
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
            ).forEach(persons ->
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
                connection ->Left.of(GeneralFailure.of("Wrong result!"))
            ).recover(failure -> {
                assertEquals(
                    "GeneralFailure(IllegalArgumentException, EXCEPTION -> " +
                        "java.lang.IllegalArgumentException: " +
                        "wrong number of arguments)",
                    failure.toString()
                );
                return 1;
            })
        );
    }

    @Test
    public void testQueryPreparedAsPerson() {
        checkDbCommand(
            RepositoryMagic.queryPreparedAs(
                Person.class,
                "SELECT id, name, age FROM person WHERE age < ?",
                ps -> ps.setInt(1, 40)
            ).forEach(persons ->
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
            RepositoryMagic.insert(jaredDoe).then(
                selectSingleAsPerson(3)
            ).forEach(person ->
                assertEquals(jaredDoeInserted, person)
            )
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
