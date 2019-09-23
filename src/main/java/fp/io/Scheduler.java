package fp.io;

public abstract class Scheduler {
    public abstract State getState();
    public abstract Scheduler updateState();
    
    public static interface State {}
    
    public class End implements State {}
    public class Execution implements State {}
    public class Wait implements State {
        public final long nanoSecond;
        
        public Wait(long nanoSecond) {
            this.nanoSecond = nanoSecond;
        }
    }
    
    public static class Counter extends Scheduler {
        private final int count;
        
        public Counter(int count) {
            this.count = count;
        }

        @Override
        public State getState() {
            return count <= 0 ? new End() : new Execution();
        }

        @Override
        public Scheduler updateState() {
            return new Counter(count - 1);
        }
    }
}
