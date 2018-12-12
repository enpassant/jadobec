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
        assertEquals(Right.of(Person.of(3, "Jake Doe", 13)), personOrFailure);
    }

    @Test
    public void testPersonAsRecord() {
        final Person person = Person.of(3, "Jake Doe", 13);
        final Record record = Record.build(builder -> builder
            .field("id", 3)
            .field("name", "Jake Doe")
            .field("age", 13)
        );
        System.out.println("Record: " + record);

        final Either<Failure, Record> recordOrFailure = Record.from(person);
        assertEquals(Right.of(record), recordOrFailure);
    }
}
