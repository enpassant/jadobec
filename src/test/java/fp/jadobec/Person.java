package fp.jadobec;

import java.util.Objects;

public class Person
{
    private final Integer id;

    private final String name;

    private final int age;

    private Person(final Integer id, final String name, final int age)
    {
        this.id = id;
        this.name = name;
        this.age = age;
    }

    public static Person of(final Integer id, final String name, final int age)
    {
        return new Person(id, name, age);
    }

    public static Person ofNew(final String name, final int age)
    {
        return new Person(null, name, age);
    }

    @Override
    public String toString()
    {
        return "Person(" + id + ", " + name + ", " + age + ")";
    }

    @Override
    public boolean equals(Object other)
    {
        if (other instanceof Person person) {
            return Objects.equals(id, person.id)
                && Objects.equals(name, person.name)
                && Objects.equals(age, person.age);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(id, name, age);
    }
}
