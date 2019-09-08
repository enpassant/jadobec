package fp.io;

import java.text.MessageFormat;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;

import fp.util.Either;
import fp.util.Failure;
import fp.util.Right;

public class ModuleTest {
    final static DefaultPlatform platform = new DefaultPlatform();

    final Runtime<Void> defaultVoidRuntime =
        new DefaultRuntime<Void>(null, platform);

    @AfterClass
    public static void setUp() {
        platform.shutdown();
    }

    private class TestConsole implements Console.Service {
        private final StringBuilder sb = new StringBuilder();
        private final String[] inputs;
        private int inputIndex = 0;

        public TestConsole(final String... inputs) {
            this.inputs = inputs;
        }

        public IO<Object, Object, Void> println(String line) {
            return IO.effectTotal(() -> { sb.append(line).append("\n"); });
        }
        public IO<Object, Failure, String> readLine() {
            return IO.effect(() -> inputs[inputIndex++]);
        }
        public String getOutputs() {
            return sb.toString();
        }
    }

    private class TestLog implements Log.Service {
        private final StringBuilder sb = new StringBuilder();

        public IO<Object, Object, Void> debug(String message, Object... params) {
            return IO.effectTotal(() -> {
                sb.append("[Debug] ")
                    .append(MessageFormat.format(message, params))
                    .append("\n");
            });
        }
        public String getOutputs() {
            return sb.toString();
        }
    }

    @Test
    public void testModules() {
        IO<Environment, Object, String> io =
            Console.println("Good morning, what is your name?").flatMap(v ->
            Console.readLine().flatMap(name ->
            Log.debug("User name: {0}", name).flatMap(v2 ->
            Console.println("Good to meet you, " + name + "!").map(v3 ->
            name
        ))));

        final TestConsole testConsole = new TestConsole("John");
        final TestLog testLog = new TestLog();

        final Environment environment =
            //Environment.of(Console.Service.class, new Console.Live())
                //.and(Log.Service.class, new Log.Live());
            Environment.of(Console.Service.class, testConsole)
                .and(Log.Service.class, testLog);

        final Either<Object, String> name =
            defaultVoidRuntime.unsafeRun(io.provide(environment));

        Assert.assertEquals(Right.of("John"), name);
        Assert.assertEquals(
            "Good morning, what is your name?\n" + "Good to meet you, John!\n",
            testConsole.getOutputs()
        );
        Assert.assertEquals(
            "[Debug] User name: John\n",
            testLog.getOutputs()
        );
    }
}
