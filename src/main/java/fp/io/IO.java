package fp.io;

import java.util.function.Function;
import java.util.function.Supplier;

import fp.util.Either;
import fp.util.GeneralFailure;
import fp.util.Left;
import fp.util.Right;
import fp.util.ThrowingSupplier;

public abstract class IO<C, F, R> {
	Tag tag;
	
	public static <C> IO<C, Void, C> access(Function<C, C> fn) {
        return new Access<C>(fn);
    }
    
	public static <C> IO<C, Void, Void> provide(C c) {
        return new Provide<C>(c);
    }
    
	public static <R> IO<Void, Void, R> pure(R r) {
        return new Pure<R>(r);
    }
    
	public static <R> IO<Void, Void, R> effectTotal(Supplier<R> supplier) {
        return new EffectTotal<R>(supplier);
    }
    
	public static <F extends Throwable, R> IO<Void, F, R> effectPartial(ThrowingSupplier<R, F> supplier) {
        return new EffectPartial<F, R>(supplier);
    }
    
	public <F2, R2> IO<C, F, R2> flatMap(Function<R, IO<C, F2, R2>> fn) {
        return new FlatMap<C, F, F2, R, R2>(this, fn);
    }
    
    @SuppressWarnings("unchecked")
	public <F2, R2> Either<F2, R2> evaluate(C context) {
    	switch (this.tag) {
		case Access:
			return (Either<F2, R2>) Right.of(((Access<C>) this).fn.apply(context));
		case Pure:
			return (Either<F2, R2>) Right.of(((Pure<R2>) this).r);
		case EffectTotal:
			return (Either<F2, R2>) Right.of(((EffectTotal<R2>) this).supplier.get());
		case EffectPartial:
			R result;
			try {
				result = ((EffectPartial<Throwable, R>) this).supplier.get();
				return (Either<F2, R2>) Right.of(result);
			} catch (Throwable e) {
				return (Either<F2, R2>) Left.of(e);
			}
		case FlatMap:
			final FlatMap<C, F, F2, R, R2> flatmapIO = (FlatMap<C, F, F2, R, R2>) this;
			return ((Either<F2, R>) flatmapIO.io.evaluate(context)).flatMap(
				r -> flatmapIO.fn.apply(r).evaluate(context)
			);
		default:
			return (Either<F2, R2>) Left.of(GeneralFailure.of("Interrupt"));
    	}
    }
    
	enum Tag {
		Access,
		Provide,
		Pure,
		EffectTotal,
		EffectPartial,
		FlatMap
	}
	
    private static class Access<C> extends IO<C, Void, C> {
    	final Function<C, C> fn;
    	public Access(Function<C, C> fn) {
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
	
    private static class Pure<R> extends IO<Void, Void, R> {
    	final R r;
    	public Pure(R r) {
    		tag = Tag.Pure;
    		this.r = r;
		}
    }
	
    private static class EffectTotal<R> extends IO<Void, Void, R> {
    	final Supplier<R> supplier;
    	
    	public EffectTotal(Supplier<R> supplier) {
    		tag = Tag.EffectTotal;
    		this.supplier = supplier;
		}
    }
	
    private static class EffectPartial<F extends Throwable, R> extends IO<Void, F, R> {
    	final ThrowingSupplier<R, F> supplier;
    	
    	public EffectPartial(ThrowingSupplier<R, F> supplier) {
    		tag = Tag.EffectPartial;
    		this.supplier = supplier;
		}
    }
	
    private static class FlatMap<C, F, F2, R, R2> extends IO<C, F, R2> {
    	final IO<C, F, R> io;
    	final Function<R, IO<C, F2, R2>> fn;
    	
    	public FlatMap(IO<C, F, R> io, Function<R, IO<C, F2, R2>> fn) {
    		tag = Tag.FlatMap;
    		this.io = io;
    		this.fn = fn;
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
