package jadobec;

import static org.junit.Assert.*;
import org.junit.Test;

import java.util.Arrays;
import java.util.Objects;

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
    public void testRecordAsPerson() {
        final Record record = Record.build(builder -> builder
            .field("id", 3)
            .field("name", "Jake Doe")
            .field("age", 13)
        );
        System.out.println("Record: " + record);

        final Either<Failure, Person> personOrFailure =
            record.as(Person.class);
        assertEquals(Right.of(new Person(3, "Jake Doe", 13)), personOrFailure);
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

    public static class Person {
        private final int id;
        private final String name;
        private final int age;

        Person(final int id, final String name, final int age) {
            this.id = id;
            this.name = name;
            this.age = age;
        }

        @Override
        public String toString() {
            return "Person(" + id + ", " + name + ", " + age + ")";
        }

        @Override
        public boolean equals(Object other) {
            if (other instanceof Person) {
                Person person = (Person) other;
                return Objects.equals(id, person.id)
                    && Objects.equals(name, person.name)
                    && Objects.equals(age, person.age);
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, name, age);
        }
    }

}
