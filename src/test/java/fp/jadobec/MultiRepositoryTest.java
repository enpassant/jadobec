package fp.jadobec;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import fp.io.Cause;
import fp.io.DefaultPlatform;
import fp.io.DefaultRuntime;
import fp.io.IO;
import fp.io.Runtime;
import fp.util.Either;
import fp.util.Failure;
import fp.util.Tuple2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MultiRepositoryTest
{
    static {
        System.setProperty(
            "java.util.logging.config.file",
            ClassLoader.getSystemResource(
                "logging.properties"
            ).getPath()
        );
    }

    final static DefaultPlatform platform = new DefaultPlatform();

    final static Runtime defaultRuntime =
        new DefaultRuntime(null, platform);

    @AfterAll
    public static void setUp()
    {
        platform.shutdown();
    }

    private final Person janeDoe = Person.of(2, "Jane Doe", 28);

    private final Person jakeDoe = Person.of(2, "Jake Doe", 28);

    @Test
    public void testUpdatePreparedPerson()
    {
        checkDbCommand(
            Repository.use(
                    updatePersonName(2, "Jake Doe").flatMap(v ->
                        selectSingleAsPerson(2)
                    )
                ).useContext("db1", Repository.Service.class)
                .peek(person ->
                    assertEquals(jakeDoe, person)
                ).flatMap(person ->
                    Repository.use(
                        selectSingleAsPerson(2)
                    ).useContext("db2", Repository.Service.class)
                ).peek(person ->
                    assertEquals(janeDoe, person)
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
                repository2 ->
                    Cause.resultFlatten(defaultRuntime.unsafeRun(
                        Repository.use(
                            MultiRepositoryTest.fill().flatMap(i ->
                                    Repository.use(
                                        MultiRepositoryTest.fill().flatMap(
                                            j -> testDbCommand
                                        )))
                                .provide("db2", Repository.Service.class, repository2)
                        ).provide("db1", Repository.Service.class, repository)
                    ))));

        assertTrue(
            repositoryOrFailure.isRight(),
            repositoryOrFailure.toString()
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
