package fp.io;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import fp.util.Either;
import fp.util.ExceptionFailure;
import fp.util.Failure;
import fp.util.GeneralFailure;
import fp.util.Left;
import fp.util.Right;

public class FiberContext<F, R> {
	private Object context;
	private IO<Object, ?, ?> curIo;
	private Object value = null;
	private Object valueLast = null;
	private AtomicReference<FiberState<F, R>> state =
		new AtomicReference<>(new Executing<F, R>(FiberStatus.Running, new ArrayList<>()));
			
	Deque<Function<?, IO<Object, ?, ?>>> stack =
		new ArrayDeque<Function<?, IO<Object, ?, ?>>>();


    public FiberContext(Object context) {
		super();
		this.context = context;
	}

    @SuppressWarnings("unchecked")
	public <F2, R2> Either<F, R> evaluate(IO<Object, F, R> io) {
    	CompletableFuture<Either<F, R>> future = new CompletableFuture<Either<F, R>>();
    	register(future);
    	evaluateNow(io);
    	return (Either<F, R>) ExceptionFailure.tryCatch(() -> future.get()).flatten();
    }

    public <F2, R2> Future<Either<F, R>> runAsync(IO<Object, F, R> io) {
    	CompletableFuture<Either<F, R>> future = new CompletableFuture<Either<F, R>>();
    	register(future);
    	evaluateNow(io);
    	return future;
    }

	@SuppressWarnings("unchecked")
	public <F2, R2> void evaluateNow(IO<Object, F, R> io) {
		curIo = io;
		
    	while (curIo != null) {
            switch (curIo.tag) {
                case Absolve:
                    final IO.Absolve<Object, F, R> absolveIO = (IO.Absolve<Object, F, R>) curIo;
                    stack.push((Either<F, R> v) -> v.isLeft() ?
                        IO.fail(v.left().get()) :
                        IO.succeed(v.right().get())
                    );
                    stack.push(v -> absolveIO.io);
                    break;
                case Access:
                    value = ((IO.Access<Object, F, R>) curIo).fn.apply(context);
                    break;
                case Pure:
                    value = ((IO.Succeed<Object, F, R>) curIo).r;
                    break;
                case Fail:
                    unwindStack(stack);
                    if (stack.isEmpty()) {
                        done(Left.of(((IO.Fail<Object, F, R>) curIo).f));
                        return;
                    }
                    value = ((IO.Fail<Object, F, R>) curIo).f;
                    break;
                case Fold: {
                    final IO.Fold<Object, F, F2, R2, R> foldIO = (IO.Fold<Object, F, F2, R2, R>) curIo;
                    stack.push((Function<?, IO<Object, ?, ?>>) curIo);
                    stack.push(v -> foldIO.io);
                    break;
                }
                case EffectTotal:
                    value = ((IO.EffectTotal<Object, F, R>) curIo).supplier.get();
                    break;
                case EffectPartial: {
                    try {
                        value = ((IO.EffectPartial<Object, Throwable, R>) curIo).supplier.get();
                    } catch (Throwable e) {
                        stack.push(v -> IO.fail(e));
                    }
                    break;
                }
                case Bracket: {
                    final IO.Bracket<Object, F, R2, R, Object> bracketIO = (IO.Bracket<Object, F, R2, R, Object>) curIo;
                    Either<F, R2> resource = new FiberContext<F, R2>(context).evaluate(bracketIO.acquire);
                    Either<F, R> valueBracket = (Either<F, R>) resource.flatMap(a -> {
                        final Either<F, R> result = new FiberContext<F, R>(context).evaluate(bracketIO.use.apply(a));
                        new FiberContext<F, Object>(context).evaluate(bracketIO.release.apply(a));
                        return result;
                    });
                    if (valueBracket.isLeft()) {
                        stack.push(v -> IO.fail(valueBracket.left().get()));
                    } else {
                        value = valueBracket.right().get();
                    }
                    break;
                }
                case FlatMap:
                    final IO.FlatMap<Object, F2, F, R2, R> flatmapIO =
                        (IO.FlatMap<Object, F2, F, R2, R>) curIo;
                    stack.push((R2 v) -> flatmapIO.fn.apply(v));
                    stack.push(v -> flatmapIO.io);
                    break;
                case Lock:
                    final IO.Lock<Object, F, R> lockIo = (IO.Lock<Object, F, R>) curIo;
                    value = lockIo.executor.submit(() -> new FiberContext<F, R>(context).evaluate(lockIo.io));
                    break;
                case Peek:
                    final IO.Peek<Object, F, R> peekIO = (IO.Peek<Object, F, R>) curIo;
                    stack.push((R r) -> {
                        peekIO.consumer.accept(r);
                        return IO.succeed(r);
                    });
                    stack.push(v -> peekIO.io);
                    value = valueLast;
                    break;
                default:
                	done((Either<F, R>) Left.of(GeneralFailure.of("Interrupt")));
                    return;
            }

            if (stack.isEmpty()) {
                curIo = null;
            } else {
                final Function<Object, IO<Object, ?, ?>> fn =
                    (Function<Object, IO<Object, ?, ?>>) stack.pop();
                if (value instanceof Future) {
                    Future<Either<?, ?>> futureValue = (Future<Either<?, ?>>) value;
                    value = Failure.tryCatchOptional(() -> futureValue.get()).get().get();
                }
                curIo = (IO<Object, ?, ?>) fn.apply(value);
                valueLast = value;
                value = null;
            }
    	}

        if (value instanceof Future) {
            Future<Either<?, ?>> futureValue = (Future<Either<?, ?>>) value;
            value = Failure.tryCatchOptional(() -> futureValue.get()).get().get();
        }
    	done((Either<F, R>) Right.of(value));
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
				executing.notifyObservers(doneValue.value);
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
    }
    
    private static class Done<F, R> implements FiberState<F, R> {
    	final Either<F, R> value;

		public Done(Either<F, R> value) {
			this.value = value;
		}
    }    
}
