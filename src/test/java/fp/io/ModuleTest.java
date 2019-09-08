package fp.io;

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

    @Test
    public void testConsole() {
        IO<Console.Service, Object, String> io =
            Console.println("Good morning, what is your name?").flatMap(v ->
            Console.readLine().flatMap(name ->
            Console.println("Good to meet you, " + name + "!").map(v2 ->
            name
        )));

        final TestConsole testConsole = new TestConsole("John");
        final Either<Object, String> name =
            //defaultVoidRuntime.unsafeRun(io.provide(Console.live()));
            defaultVoidRuntime.unsafeRun(io.provide(testConsole));
        Assert.assertEquals(Right.of("John"), name);
        Assert.assertEquals(
            "Good morning, what is your name?\n" + "Good to meet you, John!\n",
            testConsole.getOutputs()
        );
    }
}
