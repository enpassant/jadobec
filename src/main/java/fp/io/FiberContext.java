package fp.io;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import fp.io.IO.Tag;
import fp.util.Either;
import fp.util.ExceptionFailure;
import fp.util.Failure;
import fp.util.Left;
import fp.util.Right;

public class FiberContext<F, R> implements Fiber<F, R> {
    private final Platform platform;
    private IO<Object, ?, ?> curIo;
    private Object value = null;
    private Object valueLast = null;
        private final CompletableFuture<Fiber<F, R>> observer =
            new CompletableFuture<Fiber<F, R>>();
    private final AtomicReference<FiberState<F, R>> state;

    private Deque<Object> environments = new ArrayDeque<Object>();
    private Deque<Function<?, IO<Object, ?, ?>>> stack =
        new ArrayDeque<Function<?, IO<Object, ?, ?>>>();

    private Deque<Boolean> interruptStatus = new ArrayDeque<Boolean>();
    private boolean interrupted = false;

    public FiberContext(Object context, Platform platform) {
        super();
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

    public <F2, R2> Either<Exit<F>, R> evaluate(IO<Object, F, R> io) {
        evaluateNow(io);
        return getValue();
    }

    public <F2, R2> Future<Either<Exit<F>, R>> runAsync(IO<Object, F, R> io) {
        evaluateNow(io);
        return observer.thenApply(Fiber::getCompletedValue);
    }

    @SuppressWarnings("unchecked")
    public <F2, R2> void evaluateNow(IO<Object, F, R> io) {
        curIo = io;

        try {
            while (curIo != null) {
                if (curIo.tag == Tag.Fail || !shouldInterrupt()) {
                    switch (curIo.tag) {
                        case Absolve:
                            final IO.Absolve<Object, F, R> absolveIO =
                                (IO.Absolve<Object, F, R>) curIo;
                            stack.push((Either<F, R> v) -> v.isLeft() ?
                                IO.fail(Exit.fail(v.left())) :
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
                            value = platform.getBlocking().submit(
                                () -> new FiberContext<F, R>(
                                    environments.peek(),
                                    platform
                                ).evaluate(blockIo.io)
                            );
                            curIo = nextInstr(value);
                            break;
                        }
                        case Pure:
                            value = ((IO.Succeed<Object, F, R>) curIo).r;
                            curIo = nextInstr(value);
                            break;
                        case Fail:
                            unwindStack(stack);
                            Exit<F> exit = ((IO.Fail<Object, F, R>) curIo).f;
                            if (stack.isEmpty()) {
                                done(Left.of(exit));
                                return;
                            }
                            value = exit.getValue();
                            curIo = nextInstr(value);
                            break;
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
                            } else {
                                curIo = IO.fail(Exit.fail(either.left()));
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
                            executor.submit(() -> {
                                return fiberContext.runAsync(ioValue);
                            });
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
                        case Lock:
                            final IO.Lock<Object, F, R> lockIo =
                                (IO.Lock<Object, F, R>) curIo;
                            value = lockIo.executor.submit(() -> new FiberContext<F, R>(
                                environments.peek(),
                                platform
                            ).evaluate(lockIo.io));
                            curIo = nextInstr(value);
                            break;
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
                        default:
                            curIo = IO.interrupt();
                    }
                } else {
                    curIo = IO.interrupt();
                }
            }
        } catch(Exception e) {
            done(Left.of(Exit.die(new UnsupportedOperationException(e))));
        }
    }

    @SuppressWarnings("unchecked")
    private IO<Object, F, R> nextInstr(Object value) {
        if (value instanceof Future) {
            Future<Either<Exit<F>, ?>> futureValue = (Future<Either<Exit<F>, ?>>) value;
            Either<Failure, Either<Exit<F>, ?>> valueTry =
                ExceptionFailure.tryCatch(() -> futureValue.get());
            if (valueTry.isLeft()) {
                ((ExceptionFailure) valueTry.left()).throwable.printStackTrace();
                return IO.fail(Exit.fail((F) valueTry.left()));
            }
            Either<Exit<F>, ?> either = valueTry.get();
            if (either.isLeft()) {
                return IO.fail(either.left());
            }
            value = either.get();
        }

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

    private void done(Either<Exit<F>, R> value) {
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
    public Either<Exit<F>, R> getCompletedValue() {
        final FiberState<F, R> oldState = state.get();

        final Done<F, R> done = (Done<F, R>) oldState;
        return done.value;
    }

    private Either<Exit<F>, R> getValue() {
        final FiberState<F, R> oldState = state.get();
        if (oldState instanceof Executing) {
            final Executing<F, R> executing = (Executing<F, R>) oldState;
            return ExceptionFailure.tryCatch(
                () -> executing.firstObserver().thenApply(Fiber::getCompletedValue).get()
            ).fold(
                failure -> Left.of(Exit.die((ExceptionFailure) failure)),
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
        final Either<Exit<F>, R> value;

        public Done(Either<Exit<F>, R> value) {
            this.value = value;
        }
    }

    @Override
    public <C> IO<C, F, Void> interrupt() {
        interrupted = true;
        return IO.effectTotal(() -> {});
    }

    @Override
    public <C> IO<C, F, R> join() {
        Either<Exit<F>, R> value2 = getValue();
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
