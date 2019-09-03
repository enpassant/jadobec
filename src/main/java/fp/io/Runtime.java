package fp.io;

import fp.util.Either;

public class Runtime<C> {
	C context;
	
	public Runtime(C context) {
		this.context = context;
	}

	@SuppressWarnings("unchecked")
	public <F, R> Either<F, R> unsafeRun(IO<C, F, R> io) {
		FiberContext fiberContext = new FiberContext(context);
		return fiberContext.evaluate((IO<Object, F, R>) io);
	}

	@SuppressWarnings("unchecked")
	public <F, R> Either<F, R> unsafeRunAsync(IO<C, F, R> io) {
		FiberContext fiberContext = new FiberContext(context);
		return fiberContext.evaluate((IO<Object, F, R>) io);
	}
}
