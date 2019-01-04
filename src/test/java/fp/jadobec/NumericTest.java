package fp.jadobec;

import static org.junit.Assert.*;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.stream.Stream;
import java.util.stream.Collectors;
import java.util.function.Function;

import fp.jadobec.DbCommand;
import fp.jadobec.Record;
import fp.jadobec.Repository;

import fp.util.Either;
import fp.util.Failure;
import fp.util.Tuple2;

public class NumericTest {
    private static Either<Failure, Repository> loadRepository() {
        return Repository.load(
            "org.h2.jdbcx.JdbcDataSource",
            "SELECT 1",
            Tuple2.of("URL", "jdbc:h2:mem:")
        );
    }

    @Test
    public void testNumeric() {
        final DbCommand<Integer> createAndFill =
            createNumericDb().then(
            fillNumeric("sin", x -> Math.sin(x)).then(
            fillNumeric("cos", x -> Math.cos(x)).then(
            fillNumeric("lin", x -> x))));

        final Either<Failure, Stream<Record>>
            dataOrFailure = loadRepository()
                .flatMap(repository -> repository.use(
                    createAndFill.then(
                        queryNumericData()
                    )
                ));

        assertTrue(
            dataOrFailure.toString(),
            dataOrFailure.right().isPresent()
        );
    }

    public DbCommand<Stream<Record>> queryNumericData() {
        return Repository.query(
            "SELECT l.title, d.x, d.y " +
                "FROM data d JOIN label l ON d.id_label=l.id_label " +
                "ORDER BY x",
            rs -> Record.of(rs).get()
        );
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
                //.peek(System.out::println)
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
}
