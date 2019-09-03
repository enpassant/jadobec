package fp.io;

import java.util.concurrent.Future;

import fp.util.Either;
import fp.util.ExceptionFailure;
import fp.util.Failure;

public class Runtime<C> {
	C context;
	
	public Runtime(C context) {
		this.context = context;
	}

	public <F, R> Either<F, R> unsafeRun(IO<C, F, R> io) {
		final Either<Failure, Either<F, R>> eitherValue =
			ExceptionFailure.tryCatch(() -> unsafeRunAsync(io).get());
		return eitherValue.get();
	}

	@SuppressWarnings("unchecked")
	public <F, R> Future<Either<F, R>> unsafeRunAsync(IO<C, F, R> io) {
		FiberContext<F, R> fiberContext = new FiberContext<F, R>(context);
		return fiberContext.runAsync((IO<Object, F, R>) io);
	}
}
