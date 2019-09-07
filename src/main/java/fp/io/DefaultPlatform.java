package fp.io;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ThreadFactory;

public class DefaultPlatform implements Platform {
    private static int platformCount = 0;

    private final ExecutorService blocking = Executors.newCachedThreadPool(
        new PlatformThreadFactory("io-blocking")
    );
    private final ExecutorService executor = Executors.newFixedThreadPool(
        java.lang.Runtime.getRuntime().availableProcessors(),
        new PlatformThreadFactory("io-executor")
    );
    private final ExecutorService forkJoin = new ForkJoinPool(1);

    public DefaultPlatform() {
        platformCount++;
    }

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

    class PlatformThreadFactory implements ThreadFactory {
        private final String poolName;
        private int threadCount = 0;

        public PlatformThreadFactory(String poolName) {
            this.poolName = poolName;
        }

        public Thread newThread(Runnable r) {
            threadCount++;
            return new Thread(r, poolName + "-" + platformCount + "-thread-" + threadCount);
        }
    }
}
