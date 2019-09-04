package fp.io;

import java.util.concurrent.ExecutorService;

public interface Platform {
	ExecutorService getBlocking();
	ExecutorService getExecutor();
	ExecutorService getForkJoin();
	
	void shutdown();
}
