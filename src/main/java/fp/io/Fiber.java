package fp.io;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import fp.util.Either;

public interface Fiber<F, R> {
    Either<Exit<F>, R> getValue();
    Either<Exit<F>, R> getCompletedValue();
    <C> IO<C, F, Void> interrupt();
    <C> IO<C, F, R> join();
    <R2> Future<RaceResult<F, R, R2>> raceWith(Fiber<F, R2> that);
    void register(CompletableFuture<Fiber<F, R>> observer);
}
