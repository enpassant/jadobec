package fp.io;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;

public class DefaultPlatform implements Platform {
	private final ExecutorService blocking = Executors.newCachedThreadPool();
	private final ExecutorService executor = Executors.newFixedThreadPool(
		java.lang.Runtime.getRuntime().availableProcessors()
	);
	private final ExecutorService forkJoin = new ForkJoinPool(2);
	
	@Override
	public void shutdown() {
		blocking.shutdown();
		executor.shutdown();
		forkJoin.shutdown();
	}

	@Override
	public ExecutorService getBlocking() {
		return blocking;
	}

	@Override
	public ExecutorService getExecutor() {
		return executor;
	}

	@Override
	public ExecutorService getForkJoin() {
		return forkJoin;
	}
}
