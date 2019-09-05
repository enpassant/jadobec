package fp.io;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Function;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;

import fp.util.Left;
import fp.util.Right;

public class IOTest {
	final static DefaultPlatform platform = new DefaultPlatform();
	
	final Runtime<Void> defaultVoidRuntime = new DefaultRuntime<Void>(null, platform);
	final Runtime<Object> defaultRuntime = new DefaultRuntime<Object>(null, platform);
	
	@AfterClass
    public static void setUp() {
		platform.shutdown();
    }
	
    @Test
    public void testAbsolveSuccess() {
        IO<Void, Void, Integer> io = IO.absolve(IO.succeed(Right.of(4)));
        Assert.assertEquals(Right.of(4), defaultVoidRuntime.unsafeRun(io));
    }

    @Test
    public void testAbsolveFailure() {
        IO<Void, Integer, Integer> io = IO.absolve(IO.succeed(Left.of(4)));
        Assert.assertEquals(Left.of(4), defaultVoidRuntime.unsafeRun(io));
    }

    @Test
    public void testBlocking() {
        IO<Object, Object, String> io = IO.effectTotal(
        	() -> Thread.currentThread().getName()
        ).blocking();
        Assert.assertTrue(
        	"It is not a blocking thread's name",
        	defaultRuntime.unsafeRun(io)
        		.orElse("")
        		.matches("io-blocking-\\d+-thread-\\d+")
        );
    }

    @Test
    public void testPureIO() {
        IO<Void, Void, Integer> io = IO.succeed(4);
        Assert.assertEquals(Right.of(4), defaultVoidRuntime.unsafeRun(io));
    }

    @Test
    public void testFail() {
        IO<Void, String, ?> io = IO.fail("Syntax error");
        Assert.assertEquals(Left.of("Syntax error"), defaultVoidRuntime.unsafeRun(io));
    }

    @Test
    public void testFoldSuccess() {
    	IO<Void, String, Integer> ioValue = IO.succeed(4);
        IO<Void, String, Integer> io = ioValue
            .foldM(
                v -> IO.succeed(8),
                v -> IO.succeed(v * v)
            );
        Assert.assertEquals(Right.of(16), defaultVoidRuntime.unsafeRun(io));
    }

    @Test
    public void testFoldFailure() {
        IO<Void, String, Integer> ioValue = IO.fail("Error");
        IO<Void, String, Integer> io = ioValue
            .foldM(
                v -> IO.succeed(8),
                v -> IO.succeed(v * v)
            );
        Assert.assertEquals(Right.of(8), defaultVoidRuntime.unsafeRun(io));
    }

    @Test
    public void testFork() {
        IO<Object, Object, Integer> io = IO.effectTotal(() -> 6).fork()
        	.flatMap(fiber1 -> IO.effectTotal(() -> 7).fork()
        		.flatMap(fiber2 ->
        			fiber1.join().flatMap(value1 ->
        				fiber2.join().map(value2 -> value1 * value2)
        			)
        		)
        );
        Assert.assertEquals(Right.of(42), defaultRuntime.unsafeRun(io));
    }

    @Test
    public void testBlockingFork() {
        IO<Object, Object, Integer> io = IO.effectTotal(() -> 6).blocking().fork()
        	.flatMap(fiber1 -> IO.effectTotal(() -> 7).blocking().fork()
        		.flatMap(fiber2 ->
        			fiber1.join().flatMap(value1 ->
        				fiber2.join().map(value2 -> value1 * value2)
        			)
        		)
        );
        Assert.assertEquals(Right.of(42), defaultRuntime.unsafeRun(io));
    }

    @Test
    public void testBlockingAndNoBlockingForks() {
        IO<Object, Object, String> io =
        	IO.effectTotal(() -> Thread.currentThread().getName()).fork()
		    	.flatMap(fiber1 -> IO.effectTotal(() -> Thread.currentThread().getName()).blocking().fork()
		    		.flatMap(fiber2 ->
		    			fiber1.join().flatMap(value1 ->
		    				fiber2.join().map(value2 -> value1 + "," + value2)
		    			)
		    		)
        );
        Assert.assertTrue(
        	"One of the thread's name is not good",
        	defaultRuntime.unsafeRun(io)
        		.orElse("")
        		.matches("io-executor-\\d+-thread-\\d+,io-blocking-\\d+-thread-\\d+")
        );
    }

    @Test
    public void testFlatMapIO() {
        IO<Object, Void, Integer> io = IO.succeed(4).flatMap(
            n -> IO.effectTotal(() -> n * n)
        );
        Assert.assertEquals(Right.of(16), defaultRuntime.unsafeRun(io));
    }

    @Test
    public void testEffectPartial() {
        IO<Object, Void, Integer> io = IO.effect(() -> 8 / 2).flatMap(
            (Integer n) -> IO.effectTotal(() -> n * n)
        );
        Assert.assertEquals(
            Right.of(16),
            defaultRuntime.unsafeRun(io)
        );
    }

    @Test
    public void testEffectPartialWithFailure() {
        IO<Object, Void, Integer> io = IO.effect(() -> 8 / 0).flatMap(
            (Integer n) -> IO.effectTotal(() -> n * n)
        );
        Assert.assertEquals(
            Left.of(new ArithmeticException("/ by zero")).toString(),
            defaultRuntime.unsafeRun(io).toString()
        );
    }

    @Test
    public void testContext() {
        IO<Void, Object, String> io = IO.access(
            (Integer n) -> Integer.toString(n * n)
        ).provide(4);
        Assert.assertEquals(Right.of("16"), defaultVoidRuntime.unsafeRun(io));
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
            IO.succeed(res),
            resource -> IO.effectTotal(() -> { resource.close(); return 1; }),
            resource -> IO.effectTotal(() -> resource.use(10))
        );
        Assert.assertEquals(Right.of(10), defaultVoidRuntime.unsafeRun(io));
        Assert.assertFalse(res.acquired);
    }

    @Test
    public void testNestedBracket() {
        final Resource res1 = new Resource();
        final Resource res2 = new Resource();
        final IO<Object, Void, Integer> io = IO.bracket(
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
        Assert.assertEquals(Right.of(16), defaultRuntime.unsafeRun(io));
        Assert.assertFalse(res1.acquired);
        Assert.assertFalse(res2.acquired);
    }

    private IO<Object, Void, Boolean> odd(int n) {
    	return IO.effectTotal(() -> n == 0)
            .flatMap(b -> b ? IO.succeed(false) : even(n - 1) );
    }

    private IO<Object, Void, Boolean> even(int n) {
    	return IO.effectTotal(() -> n == 0)
            .flatMap(b -> b ? IO.succeed(true) : odd(n - 1) );
    }

    @Test
    public void testMutuallyTailRecursive() {
        IO<Object, Void, Boolean> io = even(100000);
        Assert.assertEquals(Right.of(true), defaultRuntime.unsafeRun(io));
    }

    @Test
    public void testLock() {
        ExecutorService asyncExecutor = Executors.newFixedThreadPool(4);
        ExecutorService blockingExecutor = Executors.newCachedThreadPool();
        ExecutorService calcExecutor = new ForkJoinPool(2);

        Function<Integer, IO<Object, Void, Integer>> fnIo = n -> IO.effectTotal(() -> {
//        	System.out.println(n + ": " + Thread.currentThread().getName());
        	return n + 1;
        });
        IO<Object, Void, Integer> lockIo = IO.succeed(1).flatMap(n ->
            fnIo.apply(n).on(asyncExecutor).flatMap(n1 ->
                fnIo.apply(n1).on(blockingExecutor).flatMap(n2 ->
                    fnIo.apply(n2).flatMap(n3 ->
                        fnIo.apply(n3).flatMap(n4 ->
                            fnIo.apply(n4).flatMap(n5 ->
                                fnIo.apply(n5).on(calcExecutor).flatMap(fnIo)
        ))))));
        Assert.assertEquals(Right.of(8), defaultRuntime.unsafeRun(lockIo));

        asyncExecutor.shutdown();
        blockingExecutor.shutdown();
        calcExecutor.shutdown();
    }

    @Test
    public void testPeek() {
        final Resource res = new Resource();
        IO<Object, Object, Resource> io = IO.succeed(res)
            .peek(r1 -> r1.use(4))
            .peek(r2 -> r2.close());
        Assert.assertEquals(Right.of(res), defaultRuntime.unsafeRun(io));
        Assert.assertEquals(4, res.usage);
        Assert.assertEquals(false, res.acquired);
    }
}
