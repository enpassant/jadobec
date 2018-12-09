package jadobec;

import static org.junit.Assert.*;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import java.util.stream.Collectors;

import util.Either;
import util.Failure;
import util.Right;

public class RepositoryTest {

    @Test
    public void testQuerySinglePerson() {
        final Either<Failure, Repository> repositoryOrFailure = loadRepository()
        .forEach(repository -> {
            fill(repository);

            final Either<Failure, Person> personOrFailure =
                repository.querySingle(
                    "SELECT id, name, age FROM person WHERE id = 2 and age < 30",
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
                repository.update(
                    "UPDATE person SET name='Jake Doe' WHERE id = 2"
                );
            final Either<Failure, Person> personOrFailure = idOrFailure.flatMap(
                id ->
                repository.querySingleAs(
                    Person.class,
                    "SELECT id, name, age FROM person p WHERE id = 2"
                ));
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
                repository.updatePrepared(
                    "UPDATE person SET name='Jake Doe' WHERE id = ?",
                    ps -> ps.setInt(1, 2)
                );
            final Either<Failure, Person> personOrFailure = idOrFailure.flatMap(
                id ->
                repository.querySingleAs(
                    Person.class,
                    "SELECT id, name, age FROM person p WHERE id = 2"
                ));
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
                    repository.updatePrepared(
                        "UPDATE person SET name='Jake Doe' WHERE id = ?",
                        ps -> ps.setInt(1, 2)
                    ).flatMap(id ->
                        repository.updatePrepared(
                            "UPDATE person SET name='Jare Doe' WHERE id = ?",
                            ps -> ps.setInt(1, 2)
                    ))
            );
            final Either<Failure, Person> personOrFailure =
                repository.querySingleAs(
                    Person.class,
                    "SELECT id, name, age FROM person p WHERE id = 2"
                );
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
                    repository.updatePrepared(
                        "UPDATE person SET name='Jake Doe' WHERE id = ?",
                        ps -> ps.setInt(1, 2)
                    ).flatMap(id ->
                        repository.updatePrepared(
                            "UPDATE pperson SET name='Jare Doe' WHERE id = ?",
                            ps -> ps.setInt(1, 2)
                        )
                    )
            );
            final Either<Failure, Person> personOrFailure =
                repository.querySingleAs(
                    Person.class,
                    "SELECT id, name, age FROM person p WHERE id = 2"
                );
            repository.close();

            assertEquals(Right.of(new Person(2, "Jane Doe", 28)), personOrFailure);
        });

        assertTrue(repositoryOrFailure.right().isPresent());
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
