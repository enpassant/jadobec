package fp.io;

import java.util.concurrent.Future;

import fp.util.Either;
import fp.util.ExceptionFailure;
import fp.util.Failure;

public interface Runtime<C> {
	default <F, R> Either<F, R> unsafeRun(IO<C, F, R> io) {
		final Either<Failure, Either<F, R>> eitherValue =
			ExceptionFailure.tryCatch(() -> unsafeRunAsync(io).get());
		eitherValue.forEachLeft(failure ->
			System.out.println("Failure: " + failure)
		);
		return eitherValue.get();
	}

	@SuppressWarnings("unchecked")
	default <F, R> Future<Either<F, R>> unsafeRunAsync(IO<C, F, R> io) {
		FiberContext<F, R> fiberContext = createFiberContext();
		return fiberContext.runAsync((IO<Object, F, R>) io);
	}
	
	<F, R> FiberContext<F, R> createFiberContext();
}
