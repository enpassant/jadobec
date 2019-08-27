package fp.io;

import java.math.BigInteger;

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
        IO<Void, Void, Integer> io = IO.effectPartial(() -> 8 / 2).flatMap(
        	(Integer n) -> IO.effectTotal(() -> n * n)
        );
        Assert.assertEquals(
        	Right.of(16),
        	io.evaluate(null)
        );
    }

    @Test
    public void testEffectPartialWithFailure() {
        IO<Void, Void, Integer> io = IO.effectPartial(() -> 8 / 0).flatMap(
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
    
    private static class Resource {
    	private boolean acquired = true;

    	private int usage = 0;

    	public Integer use(int n) {
    		usage = usage + n;
    		return usage;
    	}
    	
    	public void close() {
    		acquired = false;
    	}
    }

    @Test
    public void testRelease() {
        final Resource res = new Resource();
		final IO<Void, Void, Integer> io = IO.bracket(
        	IO.pure(res),
        	resource -> IO.effectTotal(() -> { resource.close(); return 1; }),
        	resource -> IO.effectTotal(() -> resource.use(10))
        );
        Assert.assertEquals(Right.of(10), io.evaluate(null));
        Assert.assertFalse(res.acquired);
    }

    @Test
    public void testNestedBracket() {
        final Resource res1 = new Resource();
        final Resource res2 = new Resource();
        final IO<Void, Void, Integer> io = IO.bracket(
        	IO.effectTotal(() -> res1),
        	resource -> IO.effectTotal(() -> {
        		resource.close();
        		return 1;
        	}),
        	resource -> IO.effectTotal(() -> resource.use(10)).flatMap(n ->
	            IO.bracket(
	            	IO.effectTotal(() -> res2),
                	resource2 -> IO.effectTotal(() -> {
                		resource2.close();
                		return 1;
                	}),
                	resource2 -> IO.effectTotal(() -> n + resource2.use(6))
                )
        	)
        );
        Assert.assertEquals(Right.of(16), io.evaluate(null));
        Assert.assertFalse(res1.acquired);
        Assert.assertFalse(res2.acquired);
    }
}
