package fp.io;

import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import fp.util.Either;
import fp.util.ThrowingSupplier;

public abstract class IO<C, F, R> {
    Tag tag;

    public static <C, F, R> IO<C, F, R> absolve(IO<C, ?, Either<F, R>> io) {
        return new Absolve<C, F, R>(io);
    }

    public static <C, F, R> IO<C, F, R> access(Function<C, R> fn) {
        return new Access<C, F, R>(fn);
    }

    public static <C, F, R> IO<C, F, R> succeed(R r) {
        return new Succeed<C, F, R>(r);
    }

    public static <C, F, R> IO<C, F, R> fail(F f) {
        return new Fail<C, F, R>(f);
    }

    public <F2, R2> IO<C, F2, R2> foldM(
        Function<F, IO<C, F2, R2>> failure,
        Function<R, IO<C, F2, R2>> success
    ) {
        return new Fold<C, F, F2, R, R2>(
            this,
            failure,
            success
        );
    }

    public IO<C, F, R> peek(Consumer<R> consumer) {
        return new Peek<C, F, R>(this, consumer);
    }

    public static <C, F, R> IO<C, F, R> effectTotal(Supplier<R> supplier) {
        return new EffectTotal<C, F, R>(supplier);
    }

    public static <C, F extends Throwable, R> IO<C, F, R> effect(
        ThrowingSupplier<R, F> supplier
    ) {
        return new EffectPartial<C, F, R>(supplier);
    }

    public <F2, R2> IO<C, F2, R2> flatMap(Function<R, IO<C, F2, R2>> fn) {
        return new FlatMap<C, F, F2, R, R2>(this, fn);
    }

    public <R2> IO<C, F, R2> map(Function<R, R2> fn) {
        return new FlatMap<C, F, F, R, R2>(this, r -> IO.succeed(fn.apply(r)));
    }

    public IO<C, F, R> on(ExecutorService executor) {
        return new Lock<C, F, R>(this, executor);
    }

    public static <C, F, A, R, R2> IO<C, F, R> bracket(
        IO<C, F, A> acquire,
        Function<A, IO<C, F, R2>> release,
        Function<A, IO<C, F, R>> use
    ) {
        return new Bracket<C, F, A, R, R2>(
            acquire,
            release,
            use
        );
    }

    public <C2> IO<Void, F, R> provide(C context) {
        return new Provide<C, Void, F, R>(context, this);
    }

    enum Tag {
        Absolve,
        Access,
        Bracket,
        Pure,
        Fail,
        Fold,
        EffectTotal,
        EffectPartial,
        FlatMap,
        Lock,
        Peek,
        Provide
    }

    static class Absolve<C, F, R> extends IO<C, F, R> {
        final IO<C, ?, Either<F, R>> io;

        public Absolve(
            IO<C, ?, Either<F, R>> io
    	) {
            tag = Tag.Absolve;
            this.io = io;
        }
    }

    static class Access<C, F, R> extends IO<C, F, R> {
    	final Function<C, R> fn;
    	public Access(Function<C, R> fn) {
            tag = Tag.Access;
            this.fn = fn;
        }
    }

    static class Succeed<C, F, R> extends IO<C, F, R> {
    	final R r;
    	public Succeed(R r) {
            tag = Tag.Pure;
            this.r = r;
        }
    }

    static class Fail<C, F, R> extends IO<C, F, R> {
    	final F f;
    	public Fail(F f) {
            tag = Tag.Fail;
            this.f = f;
        }
    }

    static class EffectTotal<C, F, R> extends IO<C, F, R> {
    	final Supplier<R> supplier;

    	public EffectTotal(Supplier<R> supplier) {
            tag = Tag.EffectTotal;
            this.supplier = supplier;
        }
    }

    static class EffectPartial<C, F extends Throwable, R> extends IO<C, F, R> {
    	final ThrowingSupplier<R, F> supplier;

    	public EffectPartial(ThrowingSupplier<R, F> supplier) {
            tag = Tag.EffectPartial;
            this.supplier = supplier;
        }
    }

    static class Bracket<C, F, A, R, R2> extends IO<C, F, R> {
        IO<C, F, A> acquire;
        Function<A, IO<C, F, R2>> release;
        Function<A, IO<C, F, R>> use;

    	public Bracket(
            IO<C, F, A> acquire,
            Function<A, IO<C, F, R2>> release,
            Function<A, IO<C, F, R>> use
    	) {
            tag = Tag.Bracket;
            this.acquire = acquire;
            this.release = release;
            this.use = use;
        }
    }

    static class Fold<C, F, F2, A, R>
    	extends IO<C, F2, R>
    	implements Function<A, IO<C, F2, R>>
    {
        IO<C, F, A> io;
        Function<F, IO<C, F2, R>> failure;
        Function<A, IO<C, F2, R>> success;

    	public Fold(
            IO<C, F, A> io,
            Function<F, IO<C, F2, R>> failure,
            Function<A, IO<C, F2, R>> success
    	) {
            tag = Tag.Fold;
            this.io = io;
            this.failure = failure;
            this.success = success;
        }

        @Override
        public IO<C, F2, R> apply(A a) {
            return success.apply(a);
        }
    }

    static class FlatMap<C, F, F2, R, R2> extends IO<C, F2, R2> {
    	final IO<C, F, R> io;
    	final Function<R, IO<C, F2, R2>> fn;

    	public FlatMap(IO<C, F, R> io, Function<R, IO<C, F2, R2>> fn) {
            tag = Tag.FlatMap;
            this.io = io;
            this.fn = fn;
        }
    }

    static class Lock<C, F, R> extends IO<C, F, R> {
    	final IO<C, F, R> io;
    	final ExecutorService executor;
    	public Lock(IO<C, F, R> io, ExecutorService executor) {
            tag = Tag.Lock;
            this.io = io;
            this.executor = executor;
        }
    }

    static class Peek<C, F, R> extends IO<C, F, R> {
    	final IO<C, F, R> io;
    	final Consumer<R> consumer;

    	public Peek(IO<C, F, R> io, Consumer<R> consumer) {
            tag = Tag.Peek;
            this.io = io;
            this.consumer = consumer;
        }
    }

    static class Provide<C, C2, F, R> extends IO<C2, F, R> {
    	final C context;
    	final IO<C, F, R> next;
    	
    	public Provide(C context, IO<C, F, R> next) {
            tag = Tag.Provide;
            this.context = context;
            this.next = next;
        }
    }
}
