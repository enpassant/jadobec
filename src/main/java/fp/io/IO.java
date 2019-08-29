package fp.io;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import fp.util.Either;
import fp.util.Failure;
import fp.util.GeneralFailure;
import fp.util.Left;
import fp.util.Right;
import fp.util.ThrowingSupplier;

public abstract class IO<C, F, R> {
	Tag tag;
    
	public static <C, F, R> IO<C, F, R> absolve(IO<C, ?, Either<F, R>> io) {
        return new Absolve<C, F, R>(io);
    }
	
	public static <C, R> IO<C, Void, R> access(Function<C, R> fn) {
        return new Access<C, R>(fn);
    }
    
	public static <C> IO<C, Void, Void> provide(C c) {
        return new Provide<C>(c);
    }
    
	public static <C, F, R> IO<C, F, R> pure(R r) {
        return new Pure<C, F, R>(r);
    }
    
	public static <C, F, R> IO<C, F, R> fail(F f) {
        return new Fail<C, F, R>(f);
    }
    
	public IO<C, F, R> peek(Consumer<R> consumer) {
        return new Peek<C, F, R>(this, consumer);
    }
    
	public static <C, F, R> IO<C, F, R> effectTotal(Supplier<R> supplier) {
        return new EffectTotal<C, F, R>(supplier);
    }
    
	public static <C, F extends Throwable, R> IO<C, F, R> effectPartial(
		ThrowingSupplier<R, F> supplier
	) {
        return new EffectPartial<C, F, R>(supplier);
    }
    
	public <F2, R2> IO<C, F2, R2> flatMap(Function<R, IO<C, F2, R2>> fn) {
        return new FlatMap<C, F, F2, R, R2>(this, fn);
    }
    
	public IO<C, F, R> on(ExecutorService executor) {
        return new Lock<C, F, R>(this, executor);
    }
    
	public static <C, F, A, R> IO<C, F, R> bracket(
		IO<C, F, A> acquire,
		Function<A, IO<C, F, ?>> release,
		Function<A, IO<C, F, R>> use
	) {
        return new Bracket<C, F, A, R>(
        	acquire,
        	release,
        	use
        );
    }
    
    @SuppressWarnings("unchecked")
	public static <C, F, F2, R, R2> Either<F, R> evaluate(C context, IO<C, F, R> io) {
    	IO<C, F, R> curIo = io;
    	Object value = null;
    	
    	Deque<Function<?, IO<C, F, ?>>> stack =
    		new ArrayDeque<Function<?, IO<C, F, ?>>>();
    	
    	while (curIo != null) {
	    	switch (curIo.tag) {
			case Absolve:
				final Absolve<C, F, R> absolveIO = (Absolve<C, F, R>) curIo;
				stack.push((Either<F, R> v) -> v.isLeft() ?
					(IO<C, F, ?>) IO.fail(v.left().get()) :
					(IO<C, F, ?>) IO.pure(v.right().get())
				);
				stack.push(v -> (IO<C, F, ?>) absolveIO.io);
				break;
			case Access:
				value = ((Access<C, R>) curIo).fn.apply(context);
				break;
			case Pure:
				value = ((Pure<C, F, R>) curIo).r;
				break;
			case Fail:
				return (Either<F, R>) Left.of(((Fail<C, F, R>) curIo).f);
			case EffectTotal:
				value = ((EffectTotal<C, F, R>) curIo).supplier.get();
				break;
			case EffectPartial: {
				try {
					value = ((EffectPartial<C, Throwable, R>) curIo).supplier.get();
				} catch (Throwable e) {
					return (Either<F, R>) Left.of(e);
				}
				break;
			}
			case Bracket: {
				final Bracket<C, F, R2, R> bracketIO = (Bracket<C, F, R2, R>) curIo;
				Either<F, R2> resource = evaluate(context, bracketIO.acquire);
				return (Either<F, R>) resource.flatMap(a -> {
					final Either<F, R> result = evaluate(context, bracketIO.use.apply(a));
					evaluate(context, bracketIO.release.apply(a));
					return result;
				});
			}
			case FlatMap:
				final FlatMap<C, F2, F, R2, R> flatmapIO = (FlatMap<C, F2, F, R2, R>) curIo;
				stack.push((R2 v) -> flatmapIO.fn.apply(v));
				stack.push(v -> (IO<C, F, ?>) flatmapIO.io);
				break;
			case Lock:
				final Lock<C, F, R> lockIo = (Lock<C, F, R>) curIo;
				value = lockIo.executor.submit(() -> evaluate(context, lockIo.io));
				break;
			case Peek:
				final Peek<C, F, R> peekIO = (Peek<C, F, R>) curIo;
				stack.push((R r) -> {
					peekIO.consumer.accept(r);
					return (IO<C, F, ?>) IO.pure(r);
				});
				stack.push(v -> (IO<C, F, ?>) peekIO.io);
				break;
			default:
				return (Either<F, R>) Left.of(GeneralFailure.of("Interrupt"));
	    	}
	    	
	    	if (stack.isEmpty()) {
	    		curIo = null;
	    	} else {
	    		final Function<Object, IO<C, F, ?>> fn =
	    			(Function<Object, IO<C, F, ?>>) stack.pop();
	    		if (value instanceof Future) {
					Future<Either<?, ?>> futureValue = (Future<Either<?, ?>>) value;
					value = Failure.tryCatchOptional(() -> futureValue.get()).get().get();
				}
	    		curIo = (IO<C, F, R>) fn.apply(value);
	    	}
    	}
    	
		if (value instanceof Future) {
			Future<Either<?, ?>> futureValue = (Future<Either<?, ?>>) value;
			value = Failure.tryCatchOptional(() -> futureValue.get()).get().get();
		}
    	return (Either<F, R>) Right.of(value);
    }
    
	enum Tag {
		Absolve,
		Access,
		Bracket,
		Provide,
		Pure,
		Fail,
		EffectTotal,
		EffectPartial,
		FlatMap,
		Lock,
		Peek
	}

    private static class Absolve<C, F, R> extends IO<C, F, R> {
		final IO<C, ?, Either<F, R>> io;
    	
		public Absolve(
			IO<C, ?, Either<F, R>> io
    	) {
    		tag = Tag.Absolve;
			this.io = io;
		}
    }
	
    private static class Access<C, R> extends IO<C, Void, R> {
    	final Function<C, R> fn;
    	public Access(Function<C, R> fn) {
    		tag = Tag.Access;
    		this.fn = fn;
		}
    }
	
    private static class Provide<C> extends IO<C, Void, Void> {
    	final C c;
    	public Provide(C c) {
    		tag = Tag.Provide;
    		this.c = c;
		}
    }
	
    private static class Pure<C, F, R> extends IO<C, F, R> {
    	final R r;
    	public Pure(R r) {
    		tag = Tag.Pure;
    		this.r = r;
		}
    }
	
    private static class Fail<C, F, R> extends IO<C, F, R> {
    	final F f;
    	public Fail(F f) {
    		tag = Tag.Fail;
    		this.f = f;
		}
    }
	
    private static class EffectTotal<C, F, R> extends IO<C, F, R> {
    	final Supplier<R> supplier;
    	
    	public EffectTotal(Supplier<R> supplier) {
    		tag = Tag.EffectTotal;
    		this.supplier = supplier;
		}
    }
	
    private static class EffectPartial<C, F extends Throwable, R> extends IO<C, F, R> {
    	final ThrowingSupplier<R, F> supplier;
    	
    	public EffectPartial(ThrowingSupplier<R, F> supplier) {
    		tag = Tag.EffectPartial;
    		this.supplier = supplier;
		}
    }

    private static class Bracket<C, F, A, R> extends IO<C, F, R> {
		IO<C, F, A> acquire;
		Function<A, IO<C, F, ?>> release;
		Function<A, IO<C, F, R>> use;
    	
    	public Bracket(
			IO<C, F, A> acquire,
			Function<A, IO<C, F, ?>> release,
			Function<A, IO<C, F, R>> use
    	) {
    		tag = Tag.Bracket;
    		this.acquire = acquire;
    		this.release = release;
    		this.use = use;
		}
    }

    private static class FlatMap<C, F, F2, R, R2> extends IO<C, F2, R2> {
    	final IO<C, F, R> io;
    	final Function<R, IO<C, F2, R2>> fn;
    	
    	public FlatMap(IO<C, F, R> io, Function<R, IO<C, F2, R2>> fn) {
    		tag = Tag.FlatMap;
    		this.io = io;
    		this.fn = fn;
		}
    }
	
    private static class Lock<C, F, R> extends IO<C, F, R> {
    	final IO<C, F, R> io;
    	final ExecutorService executor;
    	public Lock(IO<C, F, R> io, ExecutorService executor) {
    		tag = Tag.Lock;
    		this.io = io;
    		this.executor = executor;
		}
    }
	
    private static class Peek<C, F, R> extends IO<C, F, R> {
    	final IO<C, F, R> io;
    	final Consumer<R> consumer;
    	
    	public Peek(IO<C, F, R> io, Consumer<R> consumer) {
    		tag = Tag.Peek;
    		this.io = io;
    		this.consumer = consumer;
		}
    }

/*
    default <T> IO<F, T> then(IO<F, T> other) {
        return connection -> this.apply(connection).flatMap(
            t -> other.apply(connection)
        );
    }

    default IO<F, R> with(Consumer<Connection> consumer) {
        return connection -> this.apply(connection).forEach(
            t-> consumer.accept(connection)
        );
    }

    default IO<F, R> forEach(Consumer<R> consumer) {
        return connection -> this.apply(connection).forEach(consumer);
    }

    default <T> IO<F, T> map(Function<R, T> mapper) {
        return connection -> this.apply(connection).map(mapper);
    }

    default <T> IO<F, T> flatMap(Function<R, IO<F, T>> mapper) {
        return connection -> this.apply(connection).flatMap(
            t -> mapper.apply(t).apply(connection)
        );
    }

    default <T> IO<F, T> flatten() {
        return connection -> this.apply(connection).flatten();
    }

    default <T> IO<F, T> recover(Function<F, T> recover) {
        return connection -> this.apply(connection).recover(recover);
    }

    @SuppressWarnings("unchecked")
	default <T, U> IO<F, Stream<Either<F, T>>> mapList(
        Function<U, IO<F, T>> mapper
    ) {
        return this.flatMap(items -> connection ->
             Right.of(((Stream<U>) items)
                .map(item -> mapper.apply(item).apply(connection))
        ));
    }

    @SuppressWarnings("unchecked")
	default <T, U> IO<F, Stream<Either<F, T>>> mapListEither(
        Function<U, IO<F, T>> mapper
    ) {
        return this.flatMap(items -> connection ->
             Right.of(((Stream<Either<F, U>>) items)
                .map(item -> item.flatMap(
                    i -> mapper.apply(i).apply(connection))
                )
        ));
    }
*/
}
