package fp.io;

import java.io.InputStream;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.logging.Logger;
import java.util.logging.LogManager;
import java.util.stream.Stream;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;

import fp.util.Either;
import fp.util.ExceptionFailure;
import fp.util.Failure;
import fp.util.GeneralFailure;
import fp.util.Left;
import fp.util.Right;
import fp.util.Tuple2;

public class HappyEyeballs {
    final static DefaultPlatform platform = new DefaultPlatform();

    final Runtime<Object> defaultRuntime = new ForkJoinRuntime<Object>(null, platform);

    static {
        final InputStream is = HappyEyeballs.class.getClassLoader()
            .getResourceAsStream("logging.properties");

        try {
            LogManager.getLogManager().readConfiguration(is);
            is.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private final static Logger LOG = Logger.getLogger(HappyEyeballs.class.getName());

    @AfterClass
    public static void setUp() {
        platform.shutdown();
    }

    public static <Object, R> IO<Object, Throwable, R> run(
        List<IO<Object, Throwable, R>> tasks,
        long delay
    ) {
        if (tasks.isEmpty()) {
            return IO.fail(Cause.fail(new IllegalArgumentException("no tasks")));
        } else if (tasks.size() == 1) {
            return tasks.get(0);
        } else {
            return tasks.get(0).race(
                run(tasks.subList(1, tasks.size()), delay).delay(delay)
            );
        }
    }

    public static <Object, R> IO<Object, Throwable, R> run2(
        List<IO<Object, Throwable, R>> tasks,
        long delay
    ) {
        if (tasks.isEmpty()) {
            return IO.fail(Cause.fail(new IllegalArgumentException("no tasks")));
        } else if (tasks.size() == 1) {
            return tasks.get(0);
        } else {
            IO<Object, Throwable, Fiber<Throwable, Object>> signalIO =
                IO.<Object, Throwable, Object>sleep(10000000000L).fork();
            return signalIO.flatMap(signal ->
                tasks.get(0).onError(f -> signal.interrupt())
                .race(
                    IO.<Object, Throwable, Object>sleep(delay).fork().flatMap(s ->
                    IO.effect(() ->
                        s.raceWith(signal).get()
                    )).flatMap(r ->
                    run2(tasks.subList(1, tasks.size()), delay)
                ))
            );
        }
    }

    public static IO<Object, Throwable, String> printSleepPrint(long sleep, String name) {
        return IO.effect(() -> LOG.info("START: " + name)).flatMap(p1 ->
            IO.sleep(sleep).flatMap(p2 ->
            IO.effect(() -> LOG.info("DONE:  " + name)).flatMap(p3 ->
            IO.succeed(name))));
    }

    public static IO<Object, Throwable, String> printSleepFail(long sleep, String name) {
        return IO.effect(() -> LOG.info("START: " + name)).flatMap(p1 ->
            IO.sleep(sleep).flatMap(p2 ->
            IO.effect(() -> LOG.info("FAIL:  " + name)).flatMap(p3 ->
            IO.fail(Cause.fail(new Exception("Fail: " + name))))));
    }

    @Test
    public void testHappyEyeballs() {
        List<IO<Object, Throwable, String>> tasks =
            Arrays.asList(
                printSleepPrint(10000000000L, "task1"),
                printSleepFail(1000000000L, "task2"),
                printSleepPrint(3000000000L, "task3"),
                printSleepPrint(2000000000L, "task4"),
                printSleepPrint(2000000000L, "task5"),
                printSleepPrint(2000000000L, "task6"),
                printSleepPrint(2000000000L, "task7")
            );
        IO<Object, Throwable, String> io = run(tasks, 2000000000L);
        Either<Cause<Throwable>, String> result = defaultRuntime.unsafeRun(io);
        result.forEachLeft(cause -> ((ExceptionFailure) cause.getFailure()).getValue().printStackTrace());
        Assert.assertEquals(Right.of("task3"), result);

        IO<Object, Throwable, String> io2 = run2(tasks, 2000000000L);
        Either<Cause<Throwable>, String> result2 = defaultRuntime.unsafeRun(io2);
        Assert.assertEquals(Right.of("task3"), result2);
    }
}
