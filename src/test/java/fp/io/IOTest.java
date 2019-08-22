package fp.io;

import org.junit.Assert;
import org.junit.Test;

import fp.util.Left;
import fp.util.Right;

public class IOTest {
    @Test
    public void testPureIO() {
        IO<Void, Void, Integer> io = IO.pure(4);
        Assert.assertEquals(Right.of(4), io.evaluate(null));
    }

    @Test
    public void testFlatMapIO() {
        IO<Void, Void, Integer> io = IO.pure(4).flatMap(
        	n -> IO.effectTotal(() -> n * n)
        );
        Assert.assertEquals(Right.of(16), io.evaluate(null));
    }

    @Test
    public void testEffectPartial() {
        IO<Void, RuntimeException, Integer> io = IO.effectPartial(() -> 8 / 2).flatMap(
        	(Integer n) -> IO.effectTotal(() -> n * n)
        );
        Assert.assertEquals(
        	Right.of(16),
        	io.evaluate(null)
        );
    }

    @Test
    public void testEffectPartialWithFailure() {
        IO<Void, RuntimeException, Integer> io = IO.effectPartial(() -> 8 / 0).flatMap(
        	(Integer n) -> IO.effectTotal(() -> n * n)
        );
        Assert.assertEquals(
        	Left.of(new ArithmeticException("/ by zero")).toString(),
        	io.evaluate(null).toString()
        );
    }

    @Test
    public void testContext() {
        IO<Integer, Void, Integer> io = IO.access(
        	(Integer n) -> n * n
        );
        Assert.assertEquals(Right.of(16), io.evaluate(4));
    }
}
