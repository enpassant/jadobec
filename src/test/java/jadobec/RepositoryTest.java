package jadobec;

import static org.junit.Assert.*;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import java.util.stream.Collectors;
import java.util.function.Consumer;

import util.Either;
import util.Failure;
import util.Left;
import util.Right;

public class RepositoryTest {

    @Test
    public void testQuerySinglePerson() {
        testWithDemoRepository(repository -> {
            final Either<Failure, Person> personOrFailure =
                repository.querySingle(
                    "SELECT id, name, age FROM person WHERE id = 2",
                    rs -> new Person(
                        rs.getInt("id"),
                        rs.getString("name"),
                        rs.getInt("age")
                    )
                );

            assertEquals(Right.of(new Person(2, "Jane Doe", 28)), personOrFailure);
        });
    }

    @Test
    public void testQuerySingleAsPerson() {
        testWithDemoRepository(repository -> {
            final Either<Failure, Person> personOrFailure =
                repository.querySingleAs(
                    Person.class,
                    "SELECT id, name, age FROM person p WHERE id = ? and age < ?",
                    2, 30
                );

            assertEquals(Right.of(new Person(2, "Jane Doe", 28)), personOrFailure);
        });
    }

    @Test
    public void testQueryAsPerson() {
        testWithDemoRepository(repository -> {
            final Either<Failure, Stream<Person>> personsOrFailure =
                repository.queryAs(
                    Person.class,
                    "SELECT id, name, age FROM person"
                );

            assertTrue(personsOrFailure.right().isPresent());
            final List<Person> persons = personsOrFailure
                .right()
                .get()
                .collect(Collectors.toList());

            final List<Person> expectedPersons = Arrays.asList(
                new Person(1, "John Doe", 32),
                new Person(2, "Jane Doe", 28)
            );
            assertEquals(expectedPersons, persons);
        });
    }

    @Test
    public void testQueryAsPersonFailed() {
        testWithDemoRepository(repository -> {
            final Either<Failure, Stream<Person>> personsOrFailure =
                repository.queryAs(
                    Person.class,
                    "SELECT id, name FROM person"
                );

            assertEquals(
                "Left(Failure(IllegalArgumentException, EXCEPTION -> java.lang.IllegalArgumentException: wrong number of arguments))",
                personsOrFailure.toString()
            );
        });
    }

    @Test
    public void testQueryPerson() {
        testWithDemoRepository(repository -> {
            final Either<Failure, Stream<Person>> personsOrFailure =
                repository.query(
                    "SELECT id, name, age FROM person",
                    rs -> new Person(
                        rs.getInt("id"),
                        rs.getString("name"),
                        rs.getInt("age")
                    )
                );

            assertTrue(personsOrFailure.right().isPresent());
            final List<Person> persons = personsOrFailure
                .right()
                .get()
                .collect(Collectors.toList());

            final List<Person> expectedPersons = Arrays.asList(
                new Person(1, "John Doe", 32),
                new Person(2, "Jane Doe", 28)
            );
            assertEquals(expectedPersons, persons);
        });
    }

    @Test
    public void testQueryPreparedAsPerson() {
        testWithDemoRepository(repository -> {
            final Either<Failure, Stream<Person>> personsOrFailure =
                repository.queryPreparedAs(
                    Person.class,
                    "SELECT id, name, age FROM person WHERE age < ?",
                    ps -> ps.setInt(1, 40)
                );

            assertTrue(personsOrFailure.right().isPresent());
            final List<Person> persons = personsOrFailure
                .right()
                .get()
                .collect(Collectors.toList());

            final List<Person> expectedPersons = Arrays.asList(
                new Person(1, "John Doe", 32),
                new Person(2, "Jane Doe", 28)
            );
            assertEquals(expectedPersons, persons);
        });
    }

    @Test
    public void testQueryPreparedPerson() {
        testWithDemoRepository(repository -> {
            final Either<Failure, Stream<Person>> personsOrFailure =
                repository.queryPrepared(
                    "SELECT id, name, age FROM person WHERE age < ?",
                    ps -> ps.setInt(1, 40),
                    rs -> new Person(
                        rs.getInt("id"),
                        rs.getString("name"),
                        rs.getInt("age")
                    )
                );

            assertTrue(personsOrFailure.right().isPresent());
            final List<Person> persons = personsOrFailure
                .right()
                .get()
                .collect(Collectors.toList());

            final List<Person> expectedPersons = Arrays.asList(
                new Person(1, "John Doe", 32),
                new Person(2, "Jane Doe", 28)
            );
            assertEquals(expectedPersons, persons);
        });
    }

    @Test
    public void testUpdatePerson() {
        testWithDemoRepository(repository -> {
            final Either<Failure, Integer> idOrFailure =
                updatePersonName(repository, 2, "Jake Doe");
            final Either<Failure, Person> personOrFailure = idOrFailure.flatMap(
                id -> selectSingleAsPerson(repository)
            );

            assertEquals(Right.of(new Person(2, "Jake Doe", 28)), personOrFailure);
        });
    }

    @Test
    public void testUpdatePreparedPerson() {
        testWithDemoRepository(repository -> {
            final Either<Failure, Integer> idOrFailure =
                updatePersonName(repository, 2, "Jake Doe");
            final Either<Failure, Person> personOrFailure = idOrFailure.flatMap(
                id -> selectSingleAsPerson(repository)
            );

            assertEquals(Right.of(new Person(2, "Jake Doe", 28)), personOrFailure);
        });
    }

    @Test
    public void testGoodTransaction() {
        testWithDemoRepository(repository -> {
            repository.runInTransaction(() ->
                updatePersonName(repository, 2, "Jake Doe").flatMap(id ->
                    updatePersonName(repository, 2, "Jare Doe")
            ));
            final Either<Failure, Person> personOrFailure =
                selectSingleAsPerson(repository);

            assertEquals(Right.of(new Person(2, "Jare Doe", 28)), personOrFailure);
        });
    }

    @Test
    public void testBadTransaction() {
        testWithDemoRepository(repository -> {
            repository.runInTransaction(() ->
                updatePersonName(repository, 2, "Jake Doe").flatMap(id ->
                    updatePersonName(repository, 2, null)
            ));
            final Either<Failure, Person> personOrFailure =
                selectSingleAsPerson(repository);

            assertEquals(Right.of(new Person(2, "Jane Doe", 28)), personOrFailure);
        });
    }

    private static Either<Failure, Integer> updatePersonName(
        Repository repository,
        int id,
        String name
    ) {
        return repository.updatePrepared(
            "UPDATE person SET name=? WHERE id = ?",
            ps -> {
                ps.setString(1, name);
                ps.setInt(2, id);
            }
        );
    }

    private static Either<Failure, Person> selectSingleAsPerson(
        Repository repository
    ) {
        return repository.querySingleAs(
            Person.class,
            "SELECT id, name, age FROM person p WHERE id = 2"
        );
    }

    private static void testWithDemoRepository( Consumer<Repository> test) {
        final Either<Failure, Repository> repositoryOrFailure = loadRepository()
            .flatMap(RepositoryTest::fill)
            .forEach(repository -> {
                test.accept(repository);

                repository.close();
            });

        assertTrue(repositoryOrFailure.right().isPresent());
    }

    private static Either<Failure, Repository> loadRepository() {
        return Repository.load(
            "org.h2.Driver",
            "jdbc:h2:mem:",
            "SELECT 1"
        );
    }

    private static Either<Failure, Repository> fill(Repository repository) {
        return repository.runInTransaction(() ->
            repository.batchUpdate(
                "CREATE TABLE person(" +
                    "id INT, " +
                    "name VARCHAR(30) NOT NULL, " +
                    "age INT" +
                ")",
                "INSERT INTO person VALUES(1, 'John Doe', 32)",
                "INSERT INTO person VALUES(2, 'Jane Doe', 28)"
            ).map(i -> repository)
        );
    }
}
