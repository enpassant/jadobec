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

import fp.util.Either;
import fp.util.ExceptionFailure;
import fp.util.Failure;
import fp.util.GeneralFailure;
import fp.util.Left;
import fp.util.Right;

public class FiberContext<F, R> implements Fiber<F, R> {
    private final Platform platform;
    private IO<Object, ?, ?> curIo;
    private Object value = null;
    private Object valueLast = null;
        private final CompletableFuture<Either<F, R>> observer =
            new CompletableFuture<Either<F, R>>();
    private final AtomicReference<FiberState<F, R>> state;

    private Deque<Object> environments = new ArrayDeque<Object>();
    private Deque<Function<?, IO<Object, ?, ?>>> stack =
        new ArrayDeque<Function<?, IO<Object, ?, ?>>>();

    public FiberContext(Object context, Platform platform) {
        super();
        if (context != null) {
            environments.push(context);
        }
        this.platform = platform;

        final List<CompletableFuture<Either<F, R>>> observers =
            new ArrayList<>();
        observers.add(observer);

        state = new AtomicReference<>(
            new Executing<F, R>(
                FiberStatus.Running, observers)
        );
    }

    public <F2, R2> Either<F, R> evaluate(IO<Object, F, R> io) {
        evaluateNow(io);
        return getValue();
    }

    public <F2, R2> Future<Either<F, R>> runAsync(IO<Object, F, R> io) {
        evaluateNow(io);
        return observer;
    }

    @SuppressWarnings("unchecked")
    public <F2, R2> void evaluateNow(IO<Object, F, R> io) {
        curIo = io;

        while (curIo != null) {
            switch (curIo.tag) {
                case Absolve:
                    final IO.Absolve<Object, F, R> absolveIO =
                        (IO.Absolve<Object, F, R>) curIo;
                    stack.push((Either<F, R> v) -> v.isLeft() ?
                        IO.fail(v.left()) :
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
                    if (stack.isEmpty()) {
                        done(Left.of(((IO.Fail<Object, F, R>) curIo).f));
                        return;
                    }
                    value = ((IO.Fail<Object, F, R>) curIo).f;
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
                        curIo = IO.fail(either.left());
                    }
                    break;
                }
                case Bracket: {
                    final IO.Bracket<Object, F, R2, R, Object> bracketIO =
                        (IO.Bracket<Object, F, R2, R, Object>) curIo;
                    Either<F, R2> resource = new FiberContext<F, R2>(
                        environments.peek(),
                        platform
                    ).evaluate(bracketIO.acquire);
                    Either<F, R> valueBracket = (Either<F, R>) resource.flatMap(a -> {
                        final Either<F, R> result = new FiberContext<F, R>(
                            environments.peek(),
                            platform)
                        .evaluate(bracketIO.use.apply(a));
                        new FiberContext<F, Object>(
                            environments.peek(),
                            platform
                        ).evaluate(bracketIO.release.apply(a));
                        return result;
                    });
                    if (valueBracket.isLeft()) {
                        curIo = IO.fail(valueBracket.left());
                    } else {
                        value = valueBracket.right();
                        curIo = nextInstr(value);
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
                    final IO.Fork<Object, F, R> forkIo = (IO.Fork<Object, F, R>) curIo;
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
                case Lock:
                    final IO.Lock<Object, F, R> lockIo = (IO.Lock<Object, F, R>) curIo;
                    value = lockIo.executor.submit(() -> new FiberContext<F, R>(
                        environments.peek(),
                        platform
                    ).evaluate(lockIo.io));
                    curIo = nextInstr(value);
                    break;
                case Peek:
                    final IO.Peek<Object, F, R> peekIO = (IO.Peek<Object, F, R>) curIo;
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
                    done((Either<F, R>) Left.of(GeneralFailure.of("Interrupt")));
                    return;
            }
        }

        if (value instanceof Future) {
            Future<Either<?, ?>> futureValue = (Future<Either<?, ?>>) value;
            value = Failure.tryCatchOptional(() -> futureValue.get()).get().get();
        }
        done((Either<F, R>) Right.of(value));
    }

    @SuppressWarnings("unchecked")
    private IO<Object, F, R> nextInstr(Object value) {
        if (stack.isEmpty()) {
            return null;
        } else {
            if (value instanceof Future) {
                Future<Either<?, ?>> futureValue = (Future<Either<?, ?>>) value;
                Either<Failure, Either<?, ?>> valueTry =
                    ExceptionFailure.tryCatch(() -> futureValue.get());
                if (valueTry.isLeft()) {
                    ((ExceptionFailure) valueTry.left()).throwable.printStackTrace();
                    return IO.fail((F) valueTry.left());
                }
                Either<?, ?> either = valueTry.get();
                if (either.isLeft()) {
                    return IO.fail((F) either.left());
                }
                value = either.get();
            }
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
            if (fn instanceof IO.Fold) {
                stack.push(((IO.Fold) fn).failure);
                unwinding = false;
            }
        }
    }

    private void done(Either<F, R> value) {
        final FiberState<F, R> oldState = state.get();
        if (oldState instanceof Executing) {
            final Executing<F, R> executing = (Executing<F, R>) oldState;
            final Done<F, R> doneValue = new Done<F, R>(value);
            if (!state.compareAndSet(oldState, doneValue)) {
                done(value);
            } else {
                executing.notifyObservers(value);
            }
        }
    }

    private void register(CompletableFuture<Either<F, R>> observer) {
        final FiberState<F, R> oldState = state.get();
        if (oldState instanceof Executing) {
            Executing<F, R> executing = (Executing<F, R>) oldState;
            if (!state.compareAndSet(oldState, executing.addObserver(observer))) {
                register(observer);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private Either<F, R> getValue() {
        final FiberState<F, R> oldState = state.get();
        if (oldState instanceof Executing) {
            final Executing<F, R> executing = (Executing<F, R>) oldState;
            return (Either<F, R>) ExceptionFailure.tryCatch(
                () -> executing.firstObserver().get()
            ).flatten();
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
        private final List<CompletableFuture<Either<F, R>>> observers;

        public Executing(FiberStatus status, List<CompletableFuture<Either<F, R>>> observers) {
            this.status = status;
            this.observers = observers;
        }

        public Executing<F, R> addObserver(CompletableFuture<Either<F, R>> observer) {
            this.observers.add(observer);
            return new Executing<F, R>(this.status, this.observers);
        }

        public void notifyObservers(Either<F, R> value) {
            observers.forEach(future -> future.complete(value));
        }

        public CompletableFuture<Either<F, R>> firstObserver() {
            return observers.get(0);
        }
    }

    private static class Done<F, R> implements FiberState<F, R> {
        final Either<F, R> value;

        public Done(Either<F, R> value) {
            this.value = value;
        }
    }

    @Override
    public IO<Object, F, R> join() {
        Either<F, R> value2 = getValue();
        return value2
            .fold(
                failure -> IO.fail(failure),
                success -> IO.succeed(success)
            );
    }
}
