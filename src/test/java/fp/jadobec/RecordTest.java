package fp.jadobec;

import org.junit.Test;

import fp.util.Either;
import fp.util.Failure;
import fp.util.Right;

import static org.junit.Assert.assertEquals;

public class RecordTest
{
    @Test
    public void testRecordCopy()
    {
        final Record expectedRecord = Record.build(builder -> builder
            .field("id", 3)
            .field("name", "Jake Doe")
            .field("age", 13)
        );

        final Record record = Record.build(builder -> builder
            .field("id", 3)
            .field("name", "Doe")
            .field("age", 10)
        );

        final Record resultRecord = record.copy(builder -> builder
            .modify("name", name -> "Jake " + name)
            .modify("age", (Integer age) -> age + 3)
        );
        assertEquals(expectedRecord, resultRecord);
    }

    @Test
    public void testRecordAsPerson()
    {
        final Record record = Record.build(builder -> builder
            .field("id", 3)
            .field("name", "Jake Doe")
            .field("age", 13)
        );

        final Either<Failure, Person> personOrFailure =
            record.as(Person.class);
        assertEquals(Right.of(Person.of(3, "Jake Doe", 13)), personOrFailure);
    }

    @Test
    public void testPersonAsRecord()
    {
        final Person person = Person.of(3, "Jake Doe", 13);
        final Record record = Record.build(builder -> builder
            .field("id", 3)
            .field("name", "Jake Doe")
            .field("age", 13)
        );

        final Either<Failure, Record> recordOrFailure = Record.from(person);
        assertEquals(Right.of(record), recordOrFailure);
    }
}
