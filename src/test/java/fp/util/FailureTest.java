package fp.util;

import java.text.MessageFormat;
import java.util.HashSet;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import fp.util.Either;
import fp.util.Failure;
import fp.util.Right;
import fp.util.Tuple2;

public class FailureTest {

    private static final String ERROR_001 = "ERROR_001";

    @Test
    public void testCreationWithOnlyCode() {
        Failure failure = Failure.of(ERROR_001);
        Assert.assertEquals(ERROR_001, failure.getCode());
    }

    @Test
    public void testCreationWithOneParam() {
        String keyOrderId = "OrderId";
        Integer orderId = 124;

        Failure failure = Failure.of(ERROR_001, keyOrderId, orderId);
        Assert.assertEquals(ERROR_001, failure.getCode());
        Assert.assertEquals(1, failure.getParamNames().size());
        Assert.assertTrue(failure.getParamNames().contains(keyOrderId));
        Assert.assertEquals(orderId, failure.getParamValue(keyOrderId));
    }

    @Test
    public void testCreationWithThreeParams() {
        String keyOrderId = "OrderId";
        Integer orderId = 124;
        String keyName = "Name";
        String name = "Teszt Elek";
        String keyPrice = "Price";
        Double price = 124.5;

        Set<String> keys = new HashSet<>();
        keys.add(keyOrderId);
        keys.add(keyName);
        keys.add(keyPrice);

        Failure failure = Failure.of(ERROR_001,
            Tuple2.of(keyOrderId, orderId),
            Tuple2.of(keyName, name),
            Tuple2.of(keyPrice, price)
        );

        Assert.assertEquals(ERROR_001, failure.getCode());
        Assert.assertEquals(keys, failure.getParamNames());
        Assert.assertEquals(orderId, failure.getParamValue(keyOrderId));
        Assert.assertEquals(name, failure.getParamValue(keyName));
        Assert.assertEquals(price, failure.getParamValue(keyPrice));
    }

    @Test
    public void testToString() {
        String keyOrderId = "OrderId";
        Integer orderId = 124;
        String keyName = "Name";
        String name = "Teszt Elek";
        String keyPrice = "Price";
        Double price = 124.5;

        Failure failure = Failure.of(ERROR_001,
            Tuple2.of(keyOrderId, orderId),
            Tuple2.of(keyName, name),
            Tuple2.of(keyPrice, price)
        );

        String expectedStr = MessageFormat.format(
            "Failure({0}, {1} -> {2}, {3} -> {4}, {5} -> {6})",
            ERROR_001,
            keyOrderId,
            orderId,
            keyName,
            name,
            keyPrice,
            price.toString()
        );

        Assert.assertEquals(expectedStr, failure.toString());
    }

    @Test
    public void testFormat() {
        String pattern = "Order({0}, customer: {1}, price: {2})";

        String keyOrderId = "OrderId";
        Integer orderId = 124;
        String keyName = "Name";
        String name = "Teszt Elek";
        String keyPrice = "Price";
        Double price = 124.5;

        Failure failure = Failure.of(ERROR_001,
            Tuple2.of(keyOrderId, orderId),
            Tuple2.of(keyName, name),
            Tuple2.of(keyPrice, price)
        );

        String expectedStr = MessageFormat.format(
            pattern,
            orderId,
            name,
            price
        );

        Assert.assertEquals(expectedStr, failure.format(pattern));
    }

    @Test
    public void testTryCatchSuccess() {
        Either<Failure, Integer> valueResult = Failure.tryCatch(ERROR_001, () ->
            100 / 4
        );

        Assert.assertEquals(Right.of(25), valueResult);
    }

    @Test
    public void testTryCatchFailed() {
        Either<Failure, Integer> valueResult = Failure.tryCatch(() ->
            100 / 0
        );

        Assert.assertTrue(valueResult.left().isPresent());

        Failure failure = valueResult.left().get();
        Assert.assertEquals("ArithmeticException", failure.getCode());
        Assert.assertEquals(1, failure.getParamNames().size());
        Assert.assertTrue(failure.getParamNames().contains(Failure.EXCEPTION));
        Assert.assertTrue(
            failure.getParamValue(Failure.EXCEPTION) instanceof ArithmeticException
        );
    }

    @Test
    public void testTryCatchFailedWithCode() {
        Either<Failure, Integer> valueResult = Failure.tryCatch(ERROR_001, () ->
            100 / 0
        );

        Assert.assertTrue(valueResult.left().isPresent());

        Failure failure = valueResult.left().get();
        Assert.assertEquals(ERROR_001, failure.getCode());
        Assert.assertEquals(1, failure.getParamNames().size());
        Assert.assertTrue(failure.getParamNames().contains(Failure.EXCEPTION));
        Assert.assertTrue(
            failure.getParamValue(Failure.EXCEPTION) instanceof ArithmeticException
        );
    }
}
