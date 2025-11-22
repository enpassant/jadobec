package fp.jadobec;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import fp.io.Cause;
import fp.io.DefaultPlatform;
import fp.io.DefaultRuntime;
import fp.io.IO;
import fp.io.Runtime;
import fp.util.Either;
import fp.util.Failure;
import fp.util.GeneralFailure;
import fp.util.Tuple2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RepositoryTest
{
    final static DefaultPlatform platform = new DefaultPlatform();

    final static Runtime defaultRuntime =
        new DefaultRuntime(null, platform);

    @AfterAll
    public static void setUp()
    {
        platform.shutdown();
    }

    private final Person johnDoe = Person.of(1, "John Doe", 32);

    private final Person janeDoe = Person.of(2, "Jane Doe", 28);

    private final Person jakeDoe = Person.of(2, "Jake Doe", 28);

    private final Person jareDoe = Person.of(2, "Jare Doe", 28);

    private final List<Person> expectedPersons = Arrays.asList(johnDoe, janeDoe);

    @Test
    public void testQuerySinglePerson()
    {
        checkDbCommand(
            Repository.querySingle(
                "SELECT id, name, age FROM person WHERE id = 2",
                RepositoryTest::extractPerson
            ).peek(person ->
                assertEquals(janeDoe, person)
            )
        );
    }

    @Test
    public void testQueryPerson()
    {
        checkDbCommand(
            Repository.query(
                "SELECT id, name, age FROM person",
                RepositoryTest::extractPerson,
                Repository::mapToList
            ).peek(persons ->
                assertEquals(expectedPersons, persons)
            )
        );
    }

    @Test
    public void testQueryPreparedPerson()
    {
        checkDbCommand(
            Repository.queryPrepared(
                "SELECT id, name, age FROM person WHERE age < ?",
                ps -> ps.setInt(1, 40),
                RepositoryTest::extractPerson,
                Repository::mapToList
            ).peek(persons ->
                assertEquals(expectedPersons, persons)
            )
        );
    }

    @Test
    public void testUpdatePreparedPerson()
    {
        checkDbCommand(
            updatePersonName(2, "Jake Doe").flatMap(v ->
                selectSingleAsPerson(2)
            ).peek(person ->
                assertEquals(jakeDoe, person)
            )
        );
    }

    @Test
    public void testGoodTransaction()
    {
        checkDbCommand(
            Repository.transaction(
                updatePersonName(2, "Jake Doe").flatMap(v ->
                    updatePersonName(2, "Jare Doe")
                )).flatMap(v ->
                selectSingleAsPerson(2)
            ).peek(person ->
                assertEquals(jareDoe, person)
            )
        );
    }

    @Test
    public void testBadTransaction()
    {
        checkDbCommand(
            Repository.transaction(
                    updatePersonName(2, "Jake Doe").flatMap(v ->
                        updatePersonName(2, null)
                    )).recover(failure -> IO.succeed(1))
                .flatMap(v ->
                    selectSingleAsPerson(2)
                ).peek(person ->
                    assertEquals(janeDoe, person)
                )
        );
    }

    @Test
    public void testTransactionCommitFailure()
    {
        checkDbCommand(
            Repository.transaction(
                Repository.update("INSERT INTO person VALUES(3, 'Big Joe', 2)")
                    .peekM(i ->
                        IO.accessM(Connection.class, connection ->
                            IO.effect(connection::close))
                    )
            ).foldCauseM(
                cause -> IO.succeed(1),
                success -> IO.fail(Cause.fail(GeneralFailure.of(success)))
            )
        );
    }

    private static IO<Failure, Integer> updatePersonName(
        final int id,
        final String name
    )
    {
        return Repository.updatePrepared(
            "UPDATE person SET name=? WHERE id = ?",
            ps -> {
                ps.setString(1, name);
                ps.setInt(2, id);
            }
        );
    }

    private static IO<Failure, Person> selectSingleAsPerson(
        final Integer id
    )
    {
        return Repository.querySingle(
            "SELECT id, name, age FROM person p WHERE id = ?",
            RepositoryTest::extractPerson,
            id
        );
    }

    private static Person extractPerson(final ResultSet rs) throws SQLException
    {
        return Person.of(
            rs.getInt("id"),
            rs.getString("name"),
            rs.getInt("age")
        );
    }

    private static <T> void checkDbCommand(
        final IO<Failure, T> testDbCommand
    )
    {
        final Either<Failure, T> repositoryOrFailure = createRepository()
            .flatMap(repository ->
                Cause.resultFlatten(defaultRuntime.unsafeRun(
                    Repository.use(
                        RepositoryTest.fill().flatMap(i -> testDbCommand)
                    ).provide(Repository.Service.class, repository)
                )));

        assertTrue(
            repositoryOrFailure.isRight(),
            repositoryOrFailure.toString()
        );
    }

    private static Either<Failure, Repository.Live> createRepository()
    {
        return Repository.Live.create(
            "org.h2.jdbcx.JdbcDataSource",
            "SELECT 1",
            Tuple2.of("URL", "jdbc:h2:mem:")
        );
    }

    private static IO<Failure, Integer> fill()
    {
        return Repository.batchUpdate(
            "CREATE TABLE person(" +
                "id INT auto_increment UNIQUE, " +
                "name VARCHAR(30) NOT NULL, " +
                "age INT" +
                ")",
            "INSERT INTO person VALUES(1, 'John Doe', 32)",
            "INSERT INTO person VALUES(2, 'Jane Doe', 28)"
        );
    }
}
