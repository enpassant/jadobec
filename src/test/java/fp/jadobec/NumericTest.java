package fp.jadobec;

import static org.junit.Assert.*;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import java.util.stream.Collectors;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

import fp.jadobec.DbCommand;
import fp.jadobec.Record;
import fp.jadobec.Repository;

import fp.util.Either;
import fp.util.Failure;
import fp.util.Left;
import fp.util.Tuple2;

public class NumericTest {
    private static Logger logger = Logger.getLogger(NumericTest.class.getSimpleName());
    private static Consumer<Object> log(Level level, String message) {
        return object -> logger.log(level, message, object);
    }

    private static final DbCommand<Integer> createAndFill =
        createNumericDb().then(
            fillNumeric("sin(x)", x -> Math.sin(x)).then(
            fillNumeric("cos(x)", x -> Math.cos(x)).then(
            fillNumeric("x", x -> x))));

    private static Either<Failure, Repository> createRepository() {
        return Repository.create(
            "org.h2.jdbcx.JdbcDataSource",
            "SELECT 1",
            Tuple2.of("URL", "jdbc:h2:mem:")
        );
    }

    @Test
    public void testNumeric() {
        checkDbCommand(
            queryNumericData()
        );
    }

    @Test
    public void testReciprocalSum() {
        checkDbCommand(
            queryNumericData()
                .map(items -> items
                    .map(NumericTest::calcReciprocal)
                    .peek(log(Level.FINEST, "Calculated reciprocal values: {0}"))
                    .filter(NumericTest::isFieldYIsRight)
                    .map(NumericTest::mapFieldYRight)
                    .reduce(BigDecimal::add)
                ).forEach(sum ->
                    assertEquals(Optional.of(new BigDecimal(-45)), sum)
                )
        );
    }

    @Test
    public void testReciprocalXSum() {
        checkDbCommand(
            queryNumericData()
                .map(items -> items
                    .filter(record ->
                        record.fieldOrElse("title", "").equals("sin(x)")
                    )
                    .peek(log(Level.FINEST, "sin(x) values: {0}"))
                    .map(NumericTest::calcReciprocal)
                    .peek(log(Level.FINEST, "Calculated 1/sin(x) values: {0}"))
                    .filter(NumericTest::isFieldYIsRight)
                    .peek(log(Level.FINEST, "Filtered 1/sin(x) right values: {0}"))
                    .map(NumericTest::mapFieldYRight)
                    .reduce(BigDecimal::add)
                ).forEach(sum ->
                    assertEquals(Optional.of(new BigDecimal(0)), sum)
                )
        );
    }

    private static Record calcReciprocal(Record record) {
        return record.copy(builder -> builder
            .modify("title", (String title) -> "1/" + title)
            .modify("y", (BigDecimal y) -> Failure.tryCatch(
                () -> BigDecimal.ONE.divide(y, RoundingMode.HALF_UP))
            )
        );
    }

    private static DbCommand<Stream<Record>> queryNumericData() {
        return Repository.query(
            "SELECT l.title, d.x, d.y " +
                "FROM data d JOIN label l ON d.id_label=l.id_label " +
                "ORDER BY x",
            rs -> Record.of(rs).get()
        );
    }

    private static final Either<Failure, BigDecimal> wrongValue =
        Left.of(Failure.of("Wrong value"));

    private static boolean isFieldYIsRight(Record record) {
        return record.fieldOrElse("y", wrongValue).isRight();
    }

    private static BigDecimal mapFieldYRight(Record record) {
        return record.fieldOrElse("y", wrongValue).get();
    }

    private static DbCommand<Integer> fillNumeric(
        final String label,
        final Function<Double, Double> fn
    ) {
        final BigDecimal three = new BigDecimal("3.0");
        final BigDecimal step = new BigDecimal("0.1");
        return insertLabel(label).flatMap(idLabel ->
            Stream
                .iterate(three.negate(), x -> x.add(step))
                .limit(1000)
                .filter(x -> x.compareTo(three) <= 0)
                .map(x ->
                    insertData(
                        idLabel,
                        x.doubleValue(),
                        fn.apply(x.doubleValue())
                    ))
                .collect(
                    Collectors.reducing(
                        DbCommand.fix(1),
                        (c1, c2) -> c1.then(c2)
                    )
                )
            );
    }

    private static DbCommand<Integer> createNumericDb() {
        return Repository.batchUpdate(
            "CREATE TABLE label(" +
                "id_label INT auto_increment, " +
                "title VARCHAR(30) NOT NULL " +
            ")",
            "CREATE TABLE data(" +
                "id_data INT auto_increment, " +
                "id_label INT NOT NULL, " +
                "x DECIMAL(10,4) NOT NULL, " +
                "y DECIMAL(10,4) NOT NULL " +
            ")"
        );
    }

    private static DbCommand<Integer> insertLabel(final String label) {
        return Repository.updatePrepared(
            "INSERT INTO label(title) values(?)",
            ps -> ps.setString(1, label)
        );
    }

    private static DbCommand<Integer> insertData(
        final int idLabel,
        final double x,
        final double y
    ) {
        return Repository.updatePrepared(
            "INSERT INTO data(id_label, x, y) values(?, ?, ?)",
            ps -> {
                ps.setInt(1, idLabel);
                ps.setDouble(2, x);
                ps.setDouble(3, y);
            }
        );
    }

    private static <T> void checkDbCommand(DbCommand<T> testDbCommand) {
        final Either<Failure, T> repositoryOrFailure = createRepository()
            .flatMap(repository ->
                repository.use(
                    createAndFill
                        .flatMap(i -> testDbCommand)
                )
            );

        assertTrue(
            repositoryOrFailure.toString(),
            repositoryOrFailure.right().isPresent()
        );
    }
}
