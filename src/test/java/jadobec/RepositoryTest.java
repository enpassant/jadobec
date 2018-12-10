package jadobec;

import static org.junit.Assert.*;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import java.util.stream.Collectors;

import util.Either;
import util.Failure;
import util.Left;
import util.Right;

public class RepositoryTest {

    @Test
    public void testQuerySinglePerson() {
        final Either<Failure, Repository> repositoryOrFailure = loadRepository()
        .forEach(repository -> {
            fill(repository);

            final Either<Failure, Person> personOrFailure =
                repository.querySingle(
                    "SELECT id, name, age FROM person WHERE id = 2",
                    rs -> new Person(
                        rs.getInt("id"),
                        rs.getString("name"),
                        rs.getInt("age")
                    )
                );
            repository.close();

            assertEquals(Right.of(new Person(2, "Jane Doe", 28)), personOrFailure);
        });

        assertTrue(repositoryOrFailure.right().isPresent());
    }

    @Test
    public void testQuerySingleAsPerson() {
        final Either<Failure, Repository> repositoryOrFailure = loadRepository()
        .forEach(repository -> {
            fill(repository);

            final Either<Failure, Person> personOrFailure =
                repository.querySingleAs(
                    Person.class,
                    "SELECT id, name, age FROM person p WHERE id = ? and age < ?",
                    2, 30
                );
            repository.close();

            assertEquals(Right.of(new Person(2, "Jane Doe", 28)), personOrFailure);
        });

        assertTrue(repositoryOrFailure.right().isPresent());
    }

    @Test
    public void testQueryAsPerson() {
        final Either<Failure, Repository> repositoryOrFailure = loadRepository()
        .forEach(repository -> {
            fill(repository);

            final Either<Failure, Stream<Person>> personsOrFailure =
                repository.queryAs(
                    Person.class,
                    "SELECT id, name, age FROM person"
                );
            repository.close();

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

        assertTrue(repositoryOrFailure.right().isPresent());
    }

    @Test
    public void testQueryAsPersonFailed() {
        final Either<Failure, Repository> repositoryOrFailure = loadRepository()
        .forEach(repository -> {
            fill(repository);

            final Either<Failure, Stream<Person>> personsOrFailure =
                repository.queryAs(
                    Person.class,
                    "SELECT id, name FROM person"
                );
            repository.close();

            assertEquals(
                "Left(Failure(IllegalArgumentException, EXCEPTION -> java.lang.IllegalArgumentException: wrong number of arguments))",
                personsOrFailure.toString()
            );
        });

        assertTrue(repositoryOrFailure.right().isPresent());
    }

    @Test
    public void testQueryPerson() {
        final Either<Failure, Repository> repositoryOrFailure = loadRepository()
        .forEach(repository -> {
            fill(repository);

            final Either<Failure, Stream<Person>> personsOrFailure =
                repository.query(
                    "SELECT id, name, age FROM person",
                    rs -> new Person(
                        rs.getInt("id"),
                        rs.getString("name"),
                        rs.getInt("age")
                    )
                );
            repository.close();

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

        assertTrue(repositoryOrFailure.right().isPresent());
    }

    @Test
    public void testQueryPreparedAsPerson() {
        final Either<Failure, Repository> repositoryOrFailure = loadRepository()
        .forEach(repository -> {
            fill(repository);

            final Either<Failure, Stream<Person>> personsOrFailure =
                repository.queryPreparedAs(
                    Person.class,
                    "SELECT id, name, age FROM person WHERE age < ?",
                    ps -> ps.setInt(1, 40)
                );
            repository.close();

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

        assertTrue(repositoryOrFailure.right().isPresent());
    }

    @Test
    public void testQueryPreparedPerson() {
        final Either<Failure, Repository> repositoryOrFailure = loadRepository()
        .forEach(repository -> {
            fill(repository);

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
            repository.close();

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

        assertTrue(repositoryOrFailure.right().isPresent());
    }

    @Test
    public void testUpdatePerson() {
        final Either<Failure, Repository> repositoryOrFailure = loadRepository()
        .forEach(repository -> {
            fill(repository);

            final Either<Failure, Integer> idOrFailure =
                updatePersonName(repository, 2, "Jake Doe");
            final Either<Failure, Person> personOrFailure = idOrFailure.flatMap(
                id -> selectSingleAsPerson(repository)
            );
            repository.close();

            assertEquals(Right.of(new Person(2, "Jake Doe", 28)), personOrFailure);
        });

        assertTrue(repositoryOrFailure.right().isPresent());
    }

    @Test
    public void testUpdatePreparedPerson() {
        final Either<Failure, Repository> repositoryOrFailure = loadRepository()
        .forEach(repository -> {
            fill(repository);

            final Either<Failure, Integer> idOrFailure =
                updatePersonName(repository, 2, "Jake Doe");
            final Either<Failure, Person> personOrFailure = idOrFailure.flatMap(
                id -> selectSingleAsPerson(repository)
            );
            repository.close();

            assertEquals(Right.of(new Person(2, "Jake Doe", 28)), personOrFailure);
        });

        assertTrue(repositoryOrFailure.right().isPresent());
    }

    @Test
    public void testGoodTransaction() {
        final Either<Failure, Repository> repositoryOrFailure = loadRepository()
        .forEach(repository -> {
            fill(repository);

            repository.runInTransaction(() ->
                updatePersonName(repository, 2, "Jake Doe").flatMap(id ->
                    updatePersonName(repository, 2, "Jare Doe")
            ));
            final Either<Failure, Person> personOrFailure =
                selectSingleAsPerson(repository);
            repository.close();

            assertEquals(Right.of(new Person(2, "Jare Doe", 28)), personOrFailure);
        });

        assertTrue(repositoryOrFailure.right().isPresent());
    }

    @Test
    public void testBadTransaction() {
        final Either<Failure, Repository> repositoryOrFailure = loadRepository()
        .forEach(repository -> {
            fill(repository);

            repository.runInTransaction(() ->
                updatePersonName(repository, 2, "Jake Doe").flatMap(id ->
                    Left.of(Failure.of("Processing failed"))
            ));
            final Either<Failure, Person> personOrFailure =
                selectSingleAsPerson(repository);
            repository.close();

            assertEquals(Right.of(new Person(2, "Jane Doe", 28)), personOrFailure);
        });

        assertTrue(repositoryOrFailure.right().isPresent());
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

    private static Either<Failure, Repository> loadRepository() {
        return Repository.load(
            "org.h2.Driver",
            "jdbc:h2:mem:test",
            "SELECT 1"
        );
    }

    private static void fill(Repository repository) {
        Arrays.asList(
            "CREATE TABLE person(id INT, name VARCHAR(30), age INT)",
            "INSERT INTO person VALUES(1, 'John Doe', 32)",
            "INSERT INTO person VALUES(2, 'Jane Doe', 28)"
        ).stream().forEach(repository::update);
    }
}
