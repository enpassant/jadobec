package jadobec;

import static org.junit.Assert.*;
import org.junit.Test;

import java.util.Arrays;
import java.util.Objects;

import util.Either;
import util.Failure;
import util.Right;

public class RecordTest {
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
