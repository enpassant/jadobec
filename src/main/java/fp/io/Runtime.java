package fp.io;

import java.util.concurrent.Future;

import fp.util.Either;
import fp.util.ExceptionFailure;
import fp.util.Failure;
import fp.util.Left;

public interface Runtime<C> {
    @SuppressWarnings("unchecked")
    default <F, R> Either<Exit<F>, R> unsafeRun(IO<C, F, R> io) {
        final Either<Failure, Either<Exit<F>, R>> eitherValue =
            ExceptionFailure.tryCatch(() -> unsafeRunAsync(io).get());
        eitherValue.forEachLeft(failure -> {
            if (failure instanceof ExceptionFailure) {
                ExceptionFailure exceptionFailure = (ExceptionFailure) failure;
                exceptionFailure.throwable.printStackTrace(System.err);
            }
        });
        return (Either<Exit<F>, R>) eitherValue.fold(
            failure -> Left.of(Exit.fail(failure)),
            success -> success
        );
    }

    @SuppressWarnings("unchecked")
    default <F, R> Future<Either<Exit<F>, R>> unsafeRunAsync(IO<C, F, R> io) {
        FiberContext<F, R> fiberContext = createFiberContext();
        return fiberContext.runAsync((IO<Object, F, R>) io);
    }

    <F, R> FiberContext<F, R> createFiberContext();
}
