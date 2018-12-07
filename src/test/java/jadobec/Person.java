package jadobec;

import java.util.Objects;

public class Person {
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
