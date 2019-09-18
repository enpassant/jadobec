package fp.io;

public interface Fiber<F, R> {
    <C> IO<C, F, R> interrupt();
    <C> IO<C, F, R> join();
}
