package jadobec;

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

import util.Either;
import util.Failure;
import util.Left;
import util.Right;
import util.Tuple2;

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
        testWithDemoConnection(connection -> {
            final Either<Failure, Person> personOrFailure =
                Repository.querySingle(
                    "SELECT id, name, age FROM person WHERE id = 2",
                    rs -> Person.of(
                        rs.getInt("id"),
                        rs.getString("name"),
                        rs.getInt("age")
                    )
                ).apply(connection);

            assertEquals(Right.of(janeDoe), personOrFailure);
        });
    }

    @Test
    public void testQuerySingleAsPerson() {
        testWithDemoConnection(connection -> {
            final Either<Failure, Person> personOrFailure =
                Repository.querySingleAs(
                    Person.class,
                    "SELECT id, name, age FROM person p WHERE id = ? and age < ?",
                    2,
                    30
                ).apply(connection);

            assertEquals(Right.of(janeDoe), personOrFailure);
        });
    }

    @Test
    public void testQueryAsPerson() {
        testWithDemoConnection(connection -> {
            final Either<Failure, Stream<Person>> personsOrFailure =
                Repository.queryAs(
                    Person.class,
                    "SELECT id, name, age FROM person"
                ).apply(connection);

            assertTrue(
                personsOrFailure.toString(),
                personsOrFailure.right().isPresent()
            );
            final List<Person> persons = personsOrFailure
                .right()
                .get()
                .collect(Collectors.toList());

            assertEquals(expectedPersons, persons);
        });
    }

    @Test
    public void testQueryAsPersonFailed() {
        testWithDemoConnection(connection -> {
            final Either<Failure, Stream<Person>> personsOrFailure =
                Repository.queryAs(
                    Person.class,
                    "SELECT id, name FROM person"
                ).apply(connection);

            assertEquals(
                "Left(Failure(IllegalArgumentException, EXCEPTION -> " +
                    "java.lang.IllegalArgumentException: " +
                    "wrong number of arguments))",
                personsOrFailure.toString()
            );
        });
    }

    @Test
    public void testQueryPerson() {
        testWithDemoConnection(connection -> {
            final Either<Failure, Stream<Person>> personsOrFailure =
                Repository.query(
                    "SELECT id, name, age FROM person",
                    rs -> Person.of(
                        rs.getInt("id"),
                        rs.getString("name"),
                        rs.getInt("age")
                    )
                ).apply(connection);

            assertTrue(
                personsOrFailure.toString(),
                personsOrFailure.right().isPresent()
            );
            final List<Person> persons = personsOrFailure
                .right()
                .get()
                .collect(Collectors.toList());

            assertEquals(expectedPersons, persons);
        });
    }

    @Test
    public void testQueryPreparedAsPerson() {
        testWithDemoConnection(connection -> {
            final Either<Failure, Stream<Person>> personsOrFailure =
                Repository.queryPreparedAs(
                    Person.class,
                    "SELECT id, name, age FROM person WHERE age < ?",
                    ps -> ps.setInt(1, 40)
                ).apply(connection);

            assertTrue(
                personsOrFailure.toString(),
                personsOrFailure.right().isPresent()
            );
            final List<Person> persons = personsOrFailure
                .right()
                .get()
                .collect(Collectors.toList());

            assertEquals(expectedPersons, persons);
        });
    }

    @Test
    public void testQueryPreparedPerson() {
        testWithDemoConnection(connection -> {
            final Either<Failure, Stream<Person>> personsOrFailure =
                Repository.queryPrepared(
                    "SELECT id, name, age FROM person WHERE age < ?",
                    ps -> ps.setInt(1, 40),
                    rs -> Person.of(
                        rs.getInt("id"),
                        rs.getString("name"),
                        rs.getInt("age")
                    )
                ).apply(connection);

            assertTrue(
                personsOrFailure.toString(),
                personsOrFailure.right().isPresent()
            );
            final List<Person> persons = personsOrFailure
                .right()
                .get()
                .collect(Collectors.toList());

            assertEquals(expectedPersons, persons);
        });
    }

    @Test
    public void testUpdatePerson() {
        testWithDemoConnection(connection -> {
            final Either<Failure, Integer> idOrFailure =
                updatePersonName(connection, 2, "Jake Doe");
            final Either<Failure, Person> personOrFailure = idOrFailure.flatMap(
                id -> selectSingleAsPerson(connection, 2)
            );

            assertEquals(Right.of(jakeDoe), personOrFailure);
        });
    }

    @Test
    public void testUpdatePreparedPerson() {
        testWithDemoConnection(connection -> {
            final Either<Failure, Integer> idOrFailure =
                updatePersonName(connection, 2, "Jake Doe");
            final Either<Failure, Person> personOrFailure = idOrFailure.flatMap(
                id -> selectSingleAsPerson(connection, 2)
            );

            assertEquals(Right.of(jakeDoe), personOrFailure);
        });
    }

    @Test
    public void testGoodTransaction() {
        testWithDemoConnection(connection -> {
            final Either<Failure, Person> personOrFailure =
                Repository.transaction(() ->
                    updatePersonName(connection, 2, "Jake Doe").flatMap(id ->
                        updatePersonName(connection, 2, "Jare Doe")
                )).apply(connection).flatMap(id ->
                    selectSingleAsPerson(connection, 2)
                );

            assertEquals(Right.of(jareDoe), personOrFailure);
        });
    }

    @Test
    public void testBadTransaction() {
        testWithDemoConnection(connection -> {
            Repository.transaction(() ->
                updatePersonName(connection, 2, "Jake Doe").flatMap(id ->
                    updatePersonName(connection, 2, null)
            )).apply(connection);
            final Either<Failure, Person> personOrFailure =
                selectSingleAsPerson(connection, 2);

            assertEquals(Right.of(janeDoe), personOrFailure);
        });
    }

    @Test
    public void testInsertPerson() {
        testWithDemoConnection(connection -> {
            final Either<Failure, Integer> idOrFailure =
                Repository.insert(jaredDoe).apply(connection);
            final Either<Failure, Person> personOrFailure = idOrFailure.flatMap(
                id -> selectSingleAsPerson(connection, 3)
            );

            assertEquals(Right.of(jaredDoeInserted), personOrFailure);
        });
    }

    private static Either<Failure, Integer> updatePersonName(
        Connection connection,
        int id,
        String name
    ) {
        return Repository.updatePrepared(
            "UPDATE person SET name=? WHERE id = ?",
            ps -> {
                ps.setString(1, name);
                ps.setInt(2, id);
            }
        ).apply(connection);
    }

    private static Either<Failure, Person> selectSingleAsPerson(
        Connection connection,
        Integer id
    ) {
        return Repository.querySingleAs(
            Person.class,
            "SELECT id, name, age FROM person p WHERE id = ?",
            id
        ).apply(connection);
    }

    private static void testWithDemoConnection(Consumer<Connection> test) {
        final Either<Failure, Integer> repositoryOrFailure = loadRepository()
            .flatMap(repository -> {
                Either<Failure, Integer> result =
                    repository.use(connection -> {
                        return RepositoryTest.fill().apply(connection)
                            .forEach(i -> test.accept(connection));
                    });

                return result;
            });

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
