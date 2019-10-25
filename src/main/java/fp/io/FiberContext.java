package fp.io;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Stream;

import fp.io.IO.Tag;
import fp.io.Scheduler.State;
import fp.util.Either;
import fp.util.ExceptionFailure;
import fp.util.Failure;
import fp.util.Left;
import fp.util.Right;

public class FiberContext<F, R> implements Fiber<F, R> {
    private static long counter = 0;
    private final long number;
    
    private final Platform platform;
    private IO<Object, ?, ?> curIo;
    private Object value = null;
    private Object valueLast = null;
    private CompletableFuture<Void> mainFuture = null;
    private final CompletableFuture<Fiber<F, R>> observer =
        new CompletableFuture<Fiber<F, R>>();
    private final AtomicReference<FiberState<F, R>> state;

    private Deque<Object> environments = new ArrayDeque<Object>();
    private Deque<Function<?, IO<Object, ?, ?>>> stack =
        new ArrayDeque<Function<?, IO<Object, ?, ?>>>();

    private Deque<Boolean> interruptStatus = new ArrayDeque<Boolean>();
    private boolean interrupted = false;
    private Thread thread = null;
    private Stream.Builder<Future<?>> streamFuture = Stream.<Future<?>>builder();
    private Stream.Builder<Fiber<?, ?>> streamFiberBuilder =
        Stream.<Fiber<?, ?>>builder();

    public FiberContext(Object context, Platform platform) {
        super();
        counter++;
        number = counter;

        if (context != null) {
            environments.push(context);
        }
        this.platform = platform;

        final List<CompletableFuture<Fiber<F, R>>> observers =
            new ArrayList<>();
        observers.add(observer);

        state = new AtomicReference<>(
            new Executing<F, R>(
                FiberStatus.Running, observers)
        );
    }

    public <F2, R2> Either<Cause<F>, R> evaluate(IO<Object, F, R> io) {
        evaluateNow(io);
        return getValue();
    }

    public <F2, R2> Future<Either<Cause<F>, R>> runAsync(IO<Object, F, R> io) {
        evaluateNow(io);
        return observer.thenApply(Fiber::getCompletedValue);
    }

    @SuppressWarnings("unchecked")
    public <F2, R2> void evaluateNow(IO<Object, F, R> io) {
        this.thread = Thread.currentThread();

        curIo = io;

        try {
            while (curIo != null) {
                if (curIo.tag == Tag.Fail || !shouldInterrupt()) {
                    switch (curIo.tag) {
                        case Absolve:
                            final IO.Absolve<Object, F, R> absolveIO =
                                (IO.Absolve<Object, F, R>) curIo;
                            stack.push((Either<F, R> v) -> v.isLeft() ?
                                IO.fail(Cause.fail(v.left())) :
                                IO.succeed(v.right())
                            );
                            curIo = absolveIO.io;
                            break;
                        case Access:
                            curIo = ((IO.Access<Object, F, R>) curIo)
                                .fn.apply(environments.peek());
                            break;
                        case Blocking: {
                            final IO.Blocking<Object, F, R> blockIo =
                                (IO.Blocking<Object, F, R>) curIo;
                            final FiberContext<F, R> fiberContext = new FiberContext<F, R>(
                                environments.peek(),
                                platform
                            );
                            streamFiberBuilder.add(fiberContext);
//                            System.out.println("Blocking Fiber " + fiberContext.number + " has started");
                            
                            value = platform.getBlocking().submit(
                                () -> fiberContext.evaluate(blockIo.io)
                            );
                            curIo = nextInstr(value);
                            break;
                        }
                        case Pure:
                            value = ((IO.Succeed<Object, F, R>) curIo).r;
                            curIo = nextInstr(value);
                            break;
                        case Fail: {
                            unwindStack(stack);
                            final Cause<F> cause = ((IO.Fail<Object, F, R>) curIo).f;
                            if (stack.isEmpty()) {
                                final Cause<F> causeNew = 
                                    (interrupted && !cause.isInterrupt()) ?
                                        cause.then(Cause.interrupt()) :
                                        cause;

                                done(Left.of(causeNew));
                                return;
                            }
                            value = cause;
                            curIo = nextInstr(value);
                            break;
                        }
                        case Fold: {
                            final IO.Fold<Object, F, F2, R2, R> foldIO =
                                (IO.Fold<Object, F, F2, R2, R>) curIo;
                            stack.push((Function<?, IO<Object, ?, ?>>) curIo);
                            curIo = foldIO.io;
                            break;
                        }
                        case EffectTotal:
                            value = ((IO.EffectTotal<Object, F, R>) curIo)
                                .supplier.get();
                            curIo = nextInstr(value);
                            break;
                        case EffectPartial: {
                            Either<Failure, R> either = ExceptionFailure.tryCatch(() ->
                                ((IO.EffectPartial<Object, Failure, R>) curIo).supplier.get()
                            );
                            if (either.isRight()) {
                                value = either.right();
                                curIo = nextInstr(value);
                            } else if (((ExceptionFailure) either.left()).throwable
                                instanceof InterruptedException
                            ) {
                                curIo = IO.fail(Cause.interrupt());
                            } else  {
                                curIo = IO.fail(Cause.fail(either.left()));
                            }
                            break;
                        }
                        case FlatMap:
                            final IO.FlatMap<Object, F2, F, R2, R> flatmapIO =
                                (IO.FlatMap<Object, F2, F, R2, R>) curIo;
                            stack.push((R2 v) -> flatmapIO.fn.apply(v));
                            curIo = flatmapIO.io;
                            break;
                        case Fork: {
                            final IO.Fork<Object, F, R> forkIo =
                                (IO.Fork<Object, F, R>) curIo;
                            final IO<Object, F, R> ioValue;
                            final ExecutorService executor;
                            if (forkIo.io.tag == IO.Tag.Blocking) {
                                ioValue = ((IO.Blocking<Object, F, R>) forkIo.io).io;
                                executor = platform.getBlocking();
                            } else {
                                ioValue = forkIo.io;
                                executor = platform.getExecutor();
                            }
                            final FiberContext<F, R> fiberContext =
                                new FiberContext<F, R>(environments.peek(), platform);
                            streamFiberBuilder.add(fiberContext);

                            platform.toCompletablePromise(
                                executor.submit(() -> {
                                    return fiberContext.runAsync(ioValue);
                                })
                            );
                            value = fiberContext;
                            curIo = nextInstr(value);
                            break;
                        }
                        case InterruptStatus:
                            final IO.InterruptStatus<Object, F, R> interruptStatusIo =
                                (IO.InterruptStatus<Object, F, R>) curIo;
                            
                            interruptStatus.push(interruptStatusIo.flag);
                            stack.push(new InterruptExit());
                            curIo = interruptStatusIo.io;
                            break;
                        case Lock: {
                            final IO.Lock<Object, F, R> lockIo =
                                (IO.Lock<Object, F, R>) curIo;
                            final FiberContext<F, R> fiberContext = new FiberContext<F, R>(
                                environments.peek(),
                                platform
                            );
                            streamFiberBuilder.add(fiberContext);
                            value = lockIo.executor.submit(
                                () -> fiberContext.evaluate(lockIo.io)
                            );
                            curIo = nextInstr(value);
                            break;
                        }
                        case Peek:
                            final IO.Peek<Object, F, R> peekIO =
                                (IO.Peek<Object, F, R>) curIo;
                            stack.push((R r) -> {
                                peekIO.consumer.accept(r);
                                return IO.succeed(r);
                            });
                            curIo = peekIO.io;
                            value = valueLast;
                            break;
                        case Provide:
                            final IO.Provide<Object, Object, F, Object> provideIO =
                                (IO.Provide<Object, Object, F, Object>) curIo;
                            environments.push(provideIO.context);
                            stack.push((R r) -> IO.effectTotal(() -> {
                                environments.pop();
                                return r;
                            }));
                            curIo = provideIO.next;
                            value = valueLast;
                            break;
                        case Schedule: {
                            final IO.Schedule<Object, F, R> scheduleIO =
                                (IO.Schedule<Object, F, R>) curIo;
                            State state = scheduleIO.scheduler.getState();
                            if (state instanceof Scheduler.Execution) {
                                curIo = scheduleIO.io.foldCauseM(
                                    scheduleIO.failure.apply(scheduleIO),
                                    scheduleIO.success.apply(scheduleIO)
                                );
                            } else if (state instanceof Scheduler.Delay) {
                                final Scheduler.Delay delay = (Scheduler.Delay) state;
                                final FiberContext<F, R> fiberContext = new FiberContext<F, R>(
                                    environments.peek(),
                                    platform
                                );
                                streamFiberBuilder.add(fiberContext);
                                value = platform.getScheduler().schedule(
                                    () -> fiberContext.evaluate(scheduleIO.io),
                                    delay.nanoSecond,
                                    TimeUnit.NANOSECONDS
                                );
                                curIo = nextInstr(value);
                            } else {
                                if (value instanceof Cause) {
                                    Cause<?> cause = (Cause<?>) value;
                                    curIo = IO.fail(cause);
                                } else {
                                    curIo = nextInstr(value);
                                }
                            }

                            break;
                        }
                        default:
                            curIo = IO.interrupt();
                    }
                } else {
                    curIo = IO.interrupt();
                }
            }
        } catch(Exception e) {
            done(Left.of(Cause.die(e)));
        }
    }

    private IO<Object, F, R> nextInstr(final Object value) {
        /*
        if (value instanceof Future) {
            @SuppressWarnings("unchecked")
            Future<Either<Cause<F>, ?>> futureValue = (Future<Either<Cause<F>, ?>>) value;
            CompletableFuture<Void> currentFuture = platform.toCompletablePromise(futureValue)
        /*/
        if (value instanceof CompletableFuture) {
            @SuppressWarnings("unchecked")
            CompletableFuture<Either<Cause<F>, ?>> futureValue =
                (CompletableFuture<Either<Cause<F>, ?>>) value;
            CompletableFuture<Void> currentFuture = futureValue
        //*/
                .thenAcceptAsync(either -> {
                    if (either.isLeft()) {
                        evaluateNow(IO.fail(either.left()));
                        return;
                    } else {
                        final IO<Object, F, R> io = nextInstrApply(either.right());
                        evaluateNow(io);
                    }
                });

            if (mainFuture == null) {
                mainFuture = currentFuture;
            }

            streamFuture.accept(futureValue);
            return null;
        } else if (value instanceof Future) {
            @SuppressWarnings("unchecked")
            Future<Either<Cause<F>, ?>> futureValue = (Future<Either<Cause<F>, ?>>) value;
            Either<Failure, Either<Cause<F>, ?>> valueTry =
                ExceptionFailure.tryCatch(() -> futureValue.get());
            if (valueTry.isLeft()) {
                return IO.fail(Cause.die((ExceptionFailure) valueTry.left()));
            }
            Either<Cause<F>, ?> either = valueTry.get();
            if (either.isLeft()) {
                this.value = either.left();
                return IO.fail(either.left());
            } else {
                this.value = either.get();
            }
        }
        
        return nextInstrApply(this.value);
    }
    
    @SuppressWarnings("unchecked")
    private IO<Object, F, R> nextInstrApply(final Object value) {
        if (stack.isEmpty()) {
            done(Right.of((R) value));
            return null;
        } else {
            final Function<Object, IO<Object, ?, ?>> fn =
                (Function<Object, IO<Object, ?, ?>>) stack.pop();
            valueLast = value;
            return (IO<Object, F, R>) fn.apply(value);
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private void unwindStack(Deque<Function<?, IO<Object, ?, ?>>> stack) {
        boolean unwinding = true;

        while(unwinding && !stack.isEmpty()) {
            final Function<?, IO<Object, ?, ?>> fn = stack.pop();
            if (fn instanceof InterruptExit) {
                popDrop(null);
            } else if (fn instanceof IO.Fold && !shouldInterrupt()) {
                stack.push(((IO.Fold) fn).failure);
                unwinding = false;
            }
        }
    }

    private void done(Either<Cause<F>, R> value) {
        final FiberState<F, R> oldState = state.get();
        if (oldState instanceof Executing) {
            final Executing<F, R> executing = (Executing<F, R>) oldState;
            final Done<F, R> doneValue = new Done<F, R>(value);
            if (!state.compareAndSet(oldState, doneValue)) {
                done(value);
            } else {
                executing.notifyObservers(this);
            }
        }
    }

    @Override
    public void register(CompletableFuture<Fiber<F, R>> observer) {
        final FiberState<F, R> oldState = state.get();
        if (oldState instanceof Executing) {
            Executing<F, R> executing = (Executing<F, R>) oldState;
            if (!state.compareAndSet(oldState, executing.addObserver(observer))) {
                register(observer);
            }
        } else {
            observer.complete(this);
        }
    }
    
    @Override
    public Either<Cause<F>, R> getCompletedValue() {
        final FiberState<F, R> oldState = state.get();

        final Done<F, R> done = (Done<F, R>) oldState;
        return done.value;
    }

    public Either<Cause<F>, R> getValue() {
        final FiberState<F, R> oldState = state.get();
        if (oldState instanceof Executing) {
            final Executing<F, R> executing = (Executing<F, R>) oldState;
            return ExceptionFailure.tryCatch(
                () -> executing.firstObserver().thenApply(Fiber::getCompletedValue).get()
            ).fold(
                failure -> Left.of(Cause.die((ExceptionFailure) failure)),
                success -> success
            );
        } else {
            final Done<F, R> done = (Done<F, R>) oldState;
            return done.value;
        }
    }

    enum FiberStatus {
        Running,
        Suspended
    }

    private static interface FiberState<F, R> { };

    private static class Executing<F, R> implements FiberState<F, R> {
        final FiberStatus status;
        private final List<CompletableFuture<Fiber<F, R>>> observers;

        public Executing(
            FiberStatus status,
            List<CompletableFuture<Fiber<F, R>>> observers
        ) {
            this.status = status;
            this.observers = observers;
        }

        public Executing<F, R> addObserver(CompletableFuture<Fiber<F, R>> observer) {
            this.observers.add(observer);
            return new Executing<F, R>(this.status, this.observers);
        }

        public void notifyObservers(Fiber<F, R> value) {
            observers.forEach(future -> future.complete(value));
        }

        public CompletableFuture<Fiber<F, R>> firstObserver() {
            return observers.get(0);
        }
    }

    private static class Done<F, R> implements FiberState<F, R> {
        final Either<Cause<F>, R> value;

        public Done(Either<Cause<F>, R> value) {
            this.value = value;
        }
    }

    @Override
    public <C> IO<C, F, Void> interrupt() {
        interrupted = true;
        
//        System.out.println("Fiber " + number + " has interrupted");

//        final Stream<Fiber<?, ?>> streamFiber = streamFiberBuilder.build();
//        streamFiber.forEach(fiber -> fiber.interrupt());
        
//        final Stream<Future<?>> stream = streamFuture.build();
//        stream.forEach(future -> future.cancel(true));
/*
        if (thread != null) {
            switch (thread.getState()) {
                case BLOCKED:
                case WAITING:
                case TIMED_WAITING:
                    thread.interrupt();
                    break;
                default:
            }
        }
        //*/
        
        if (mainFuture != null) {
            mainFuture.cancel(true);
        }
        observer.cancel(true);
        
        return IO.effectTotal(() -> {});
    }

    @Override
    public <C> IO<C, F, R> join() {
        Either<Cause<F>, R> value2 = getValue();
        return value2
            .fold(
                failure -> IO.fail(failure),
                success -> IO.succeed(success)
            );
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public <R2> Future<RaceResult<F, R, R2>> raceWith(Fiber<F, R2> that) {
        CompletableFuture<Fiber<F, Object>> winner = new CompletableFuture<>();
        ((Fiber<F, Object>) this).register(winner);
        ((Fiber<F, Object>) that).register(winner);
        return winner.thenApply(winnerFiber ->
            new RaceResult<>(this, that, winnerFiber == this)
        );
    }

    private boolean interruptible() {
        return interruptStatus.isEmpty() || interruptStatus.peek();
    }

    private boolean shouldInterrupt() {
        return interrupted && interruptible();
    }
    
    private <A> A popDrop(A a) {
        if (!interruptStatus.isEmpty()) {
            interruptStatus.pop();
        }
        return a;
    }
    
    private class InterruptExit<C> implements Function<R, IO<C, F, R>> {
        @Override
        public IO<C, F, R> apply(R v) {
            boolean isInterruptible = interruptStatus.isEmpty() ?
                true :
                interruptStatus.peek();
            
            if (isInterruptible) {
                popDrop(null);
                return IO.succeed(v);
            } else {
                return IO.effectTotal(() -> popDrop(v));
            }
        }
    }
}
