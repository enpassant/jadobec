package fp.jadobec;

import java.util.Arrays;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Test;

import fp.io.Cause;
import fp.io.DefaultPlatform;
import fp.io.DefaultRuntime;
import fp.io.IO;
import fp.io.Runtime;
import fp.util.Either;
import fp.util.Failure;
import fp.util.Tuple2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MultiRepositoryTest
{
    private static final Repository repoBase = Repository.of();

    private static final Repository repo2 = Repository.of();

    final static DefaultPlatform platform = new DefaultPlatform();

    final static Runtime defaultRuntime =
        new DefaultRuntime(null, platform);

    @AfterClass
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
    public void testUpdatePreparedPerson()
    {
        checkDbCommand(
            updatePersonName(repoBase, 2, "Jake Doe").flatMap(v ->
                selectSingleAsPerson(repoBase, 2)
            ).peek(person ->
                assertEquals(jakeDoe, person)
            ).flatMap(person ->
                selectSingleAsPerson(repo2, 2)
            ).peek(person ->
                assertEquals(janeDoe, person)
            )
        );
    }

    private static IO<Failure, Integer> updatePersonName(
        final Repository repository,
        final int id,
        final String name
    )
    {
        return repository.updatePrepared(
            "UPDATE person SET name=? WHERE id = ?",
            ps -> {
                ps.setString(1, name);
                ps.setInt(2, id);
            }
        );
    }

    private static IO<Failure, Person> selectSingleAsPerson(
        final Repository repository,
        final Integer id
    )
    {
        return repository.querySingle(
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
        final IO<Failure, T> testDbCommand
    )
    {
        final Either<Failure, T> repositoryOrFailure = createRepository("db1").flatMap(
            repository -> createRepository("db2").flatMap(
                repository2 -> {
                    return Cause.resultFlatten(defaultRuntime.unsafeRun(
                        repoBase.use(
                                MultiRepositoryTest.fill(repoBase).flatMap(i ->
                                    repo2.use(
                                        MultiRepositoryTest.fill(repo2).flatMap(
                                            j -> testDbCommand
                                        )))
                            ).provide(repoBase.name, Repository.Service.class, repository)
                            .provide(repo2.name, Repository.Service.class, repository2)
                    ));
                }));

        assertTrue(
            repositoryOrFailure.toString(),
            repositoryOrFailure.isRight()
        );
    }

    private static Either<Failure, Repository.Live> createRepository(final String name)
    {
        return Repository.Live.create(
            "org.h2.jdbcx.JdbcDataSource",
            "SELECT 1",
            Tuple2.of("URL", "jdbc:h2:mem:" + name)
        );
    }

    private static IO<Failure, Integer> fill(final Repository repository)
    {
        return repository.batchUpdate(
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
