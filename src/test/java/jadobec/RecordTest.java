package jadobec;

import static org.junit.Assert.*;
import org.junit.Test;

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
}
