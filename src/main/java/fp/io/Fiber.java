package fp.io;

public interface Fiber<F, R> {
    IO<Object, F, R> join();
}
