package fp.io;

import org.junit.Assert;
import org.junit.Test;

import fp.util.Left;
import fp.util.Right;

public class IOTest {
    @Test
    public void testPureIO() {
        IO<Void, Void, Integer> io = IO.pure(4);
        Assert.assertEquals(Right.of(4), IO.evaluate(null, io));
    }

    @Test
    public void testFlatMapIO() {
        IO<Void, Void, Integer> io = IO.pure(4).flatMap(
        	n -> IO.effectTotal(() -> n * n)
        );
        Assert.assertEquals(Right.of(16), IO.evaluate(null, io));
    }

    @Test
    public void testEffectPartial() {
        IO<Void, Void, Integer> io = IO.effectPartial(() -> 8 / 2).flatMap(
        	(Integer n) -> IO.effectTotal(() -> n * n)
        );
        Assert.assertEquals(
        	Right.of(16),
        	IO.evaluate(null, io)
        );
    }

    @Test
    public void testEffectPartialWithFailure() {
        IO<Void, Void, Integer> io = IO.effectPartial(() -> 8 / 0).flatMap(
        	(Integer n) -> IO.effectTotal(() -> n * n)
        );
        Assert.assertEquals(
        	Left.of(new ArithmeticException("/ by zero")).toString(),
        	IO.evaluate(null, io).toString()
        );
    }

    @Test
    public void testContext() {
        IO<Integer, Void, Integer> io = IO.access(
        	(Integer n) -> n * n
        );
        Assert.assertEquals(Right.of(16), IO.evaluate(4, io));
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
        Assert.assertEquals(Right.of(10), IO.evaluate(null, io));
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
        Assert.assertEquals(Right.of(16), IO.evaluate(null, io));
        Assert.assertFalse(res1.acquired);
        Assert.assertFalse(res2.acquired);
    }
    
    private IO<Void, Void, Boolean> odd(int n) {
    	return IO.effectTotal(() -> n == 0)
    		.flatMap(b -> b ? IO.pure(false) : even(n - 1) );
    }
    
    private IO<Void, Void, Boolean> even(int n) {
    	return IO.effectTotal(() -> n == 0)
    		.flatMap(b -> b ? IO.pure(true) : odd(n - 1) );
    }
    
    @Test
    public void testMutuallyTailRecursive() {
        IO<Void, Void, Boolean> io = even(1000000);
        Assert.assertEquals(Right.of(true), IO.evaluate(null, io));
    }
}
