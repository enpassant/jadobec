package fp.io;

import java.util.concurrent.Future;

import fp.util.Either;
import fp.util.ExceptionFailure;
import fp.util.Failure;
import fp.util.Left;

public interface Runtime<C> {
    @SuppressWarnings("unchecked")
    default <F, R> Either<Cause<F>, R> unsafeRun(IO<C, F, R> io) {
        final Either<Failure, Either<Cause<F>, R>> eitherValue =
            ExceptionFailure.tryCatch(() -> unsafeRunAsync(io).get());
        eitherValue.forEachLeft(failure -> {
            if (failure instanceof ExceptionFailure) {
                ExceptionFailure exceptionFailure = (ExceptionFailure) failure;
                exceptionFailure.throwable.printStackTrace(System.err);
            }
        });
        return (Either<Cause<F>, R>) eitherValue.fold(
            failure -> Left.of(Cause.fail(failure)),
            success -> success
        );
    }

    @SuppressWarnings("unchecked")
    default <F, R> Future<Either<Cause<F>, R>> unsafeRunAsync(IO<C, F, R> io) {
        FiberContext<F, R> fiberContext = createFiberContext();
        return fiberContext.runAsync((IO<Object, F, R>) io);
    }

    <F, R> FiberContext<F, R> createFiberContext();
}
