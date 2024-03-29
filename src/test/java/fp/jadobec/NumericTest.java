package fp.jadobec;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.AfterClass;
import org.junit.Test;

import fp.io.DefaultPlatform;
import fp.io.DefaultRuntime;
import fp.io.Environment;
import fp.io.Cause;
import fp.io.IO;
import fp.io.Runtime;
import fp.util.Either;
import fp.util.ExceptionFailure;
import fp.util.Failure;
import fp.util.GeneralFailure;
import fp.util.Left;
import fp.util.Tuple2;

public class NumericTest {
    final static DefaultPlatform platform = new DefaultPlatform();

    final static Runtime<Void> defaultRuntime =
        new DefaultRuntime<Void>(null, platform);

    @AfterClass
    public static void setUp() {
        platform.shutdown();
    }

    private static Logger logger = Logger.getLogger(
        NumericTest.class.getSimpleName()
    );
    private static Consumer<Object> log(
        final Level level,
        final String message
    ) {
        return object -> logger.log(level, message, object);
    }

    private static final IO<Connection, Failure, Integer> createAndFill =
        createNumericDb().flatMap(v ->
        fillNumeric("sin(x)", x -> Math.sin(x)).flatMap(s ->
        fillNumeric("cos(x)", x -> Math.cos(x)).flatMap(c ->
        fillNumeric("x", x -> x)
    )));

    private static Either<Failure, Repository.Live> createRepository() {
        return Repository.Live.create(
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
                    .peek(
                        log(Level.FINEST, "Calculated reciprocal values: {0}")
                    )
                    .filter(NumericTest::isFieldYIsRight)
                    .map(NumericTest::mapFieldYRight)
                    .reduce(BigDecimal::add)
                ).peek(sum ->
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
                    .peek(
                        log(Level.FINEST, "Filtered 1/sin(x) right values: {0}")
                    )
                    .map(NumericTest::mapFieldYRight)
                    .reduce(BigDecimal::add)
                ).peek(sum ->
                    assertEquals(Optional.of(new BigDecimal(0)), sum)
                )
        );
    }

    private static Record calcReciprocal(final Record record) {
        return record.copy(builder -> builder
            .modify("title", (String title) -> "1/" + title)
            .modify("y", (BigDecimal y) -> ExceptionFailure.tryCatch(
                () -> BigDecimal.ONE.divide(y, RoundingMode.HALF_UP))
            )
        );
    }

    private static IO<Connection, Failure, Stream<Record>> queryNumericData() {
        return Repository.query(
            "SELECT l.title, d.x, d.y " +
                "FROM data d JOIN label l ON d.id_label=l.id_label " +
                "ORDER BY x",
            rs -> Record.of(rs).get(),
            Repository::mapToStream
        );
    }

    private static final Either<Failure, BigDecimal> wrongValue =
        Left.of(GeneralFailure.of("Wrong value"));

    private static boolean isFieldYIsRight(final Record record) {
        return record.fieldOrElse("y", wrongValue).isRight();
    }

    private static BigDecimal mapFieldYRight(final Record record) {
        return record.fieldOrElse("y", wrongValue).get();
    }

    private static IO<Connection, Failure, Integer> fillNumeric(
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
                        IO.succeed(1),
                        (c1, c2) -> c1.flatMap(c -> c2)
                    )
                )
            );
    }

    private static IO<Connection, Failure, Integer> createNumericDb() {
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

    private static IO<Connection, Failure, Integer> insertLabel(
        final String label
    ) {
        return Repository.updatePrepared(
            "INSERT INTO label(title) values(?)",
            ps -> ps.setString(1, label)
        );
    }

    private static IO<Connection, Failure, Integer> insertData(
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

    private static <T> void checkDbCommand(
        final IO<Connection, Failure, T> testDbCommand
    ) {
        final Either<Failure, T> repositoryOrFailure = createRepository()
            .flatMap(repository -> {
                final Environment environment =
                    Environment.of(Repository.Service.class, repository);
                return Cause.resultFlatten(defaultRuntime.unsafeRun(
                    Repository.use(
                        createAndFill.flatMap(i ->
                        testDbCommand
                    )).provide(environment))
                );
            });

        assertTrue(
            repositoryOrFailure.toString(),
            repositoryOrFailure.isRight()
        );
    }
}
