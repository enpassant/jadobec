package fp.io;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

public interface Platform {
    ExecutorService getBlocking();
    ExecutorService getExecutor();
    ExecutorService getForkJoin();
    ScheduledExecutorService getScheduler();

    void shutdown();
}
