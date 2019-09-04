package fp.io;

public class DefaultRuntime<C> implements Runtime<C> {
	private final C context;
	private final Platform platform;
	
	public DefaultRuntime(C context, Platform platform) {
		this.context = context;
		this.platform = platform;
	}

	public <F, R> FiberContext<F, R> createFiberContext() {
		FiberContext<F, R> fiberContext = new FiberContext<F, R>(context, platform);
		return fiberContext;
	}
}
