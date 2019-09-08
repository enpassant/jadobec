package fp.io;

import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import fp.util.Either;
import fp.util.Failure;
import fp.util.Statement;
import fp.util.ThrowingStatement;
import fp.util.ThrowingSupplier;

public abstract class IO<C, F, R> {
    Tag tag;

    public static <C, F, R> IO<C, F, R> absolve(IO<C, ?, Either<F, R>> io) {
        return new Absolve<C, F, R>(io);
    }

    public static <C, F, R> IO<C, F, R> accessM(Function<C, IO<Object, F, R>> fn) {
        return new Access<C, F, R>(fn);
    }

    public static <C, F, R> IO<C, F, R> access(Function<C, R> fn) {
        return new Access<C, F, R>(r -> IO.succeed(fn.apply(r)));
    }

    public IO<C, F, R> blocking() {
        return new Blocking<C, F, R>(this);
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

    public static <C, F> IO<C, F, Void> effectTotal(Statement statement) {
        return new EffectTotal<C, F, Void>(() -> { statement.call(); return null; });
    }

    public static <C, F extends Failure> IO<C, F, Void> effect(
        ThrowingStatement<Throwable> statement
    ) {
        return new EffectPartial<C, F, Void>(() -> { statement.call(); return null; });
    }

    public static <C, F, R> IO<C, F, R> effectTotal(Supplier<R> supplier) {
        return new EffectTotal<C, F, R>(supplier);
    }

    public static <C, F extends Failure, R> IO<C, F, R> effect(
        ThrowingSupplier<R, Throwable> supplier
    ) {
        return new EffectPartial<C, F, R>(supplier);
    }

    public <F2, R2> IO<C, F2, R2> flatMap(Function<R, IO<C, F2, R2>> fn) {
        return new FlatMap<C, F, F2, R, R2>(this, fn);
    }

    public IO<C, F, Fiber<F, R>> fork() {
        return new Fork<C, F, R>(this);
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

    public <C2> IO<C2, F, R> provide(C context) {
        return new Provide<C, C2, F, R>(context, this);
    }

    enum Tag {
        Absolve,
        Access,
        Blocking,
        Bracket,
        Pure,
        Fail,
        Fold,
        Fork,
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
        final Function<C, IO<Object, F, R>> fn;
        public Access(Function<C, IO<Object, F, R>> fn) {
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

    static class EffectPartial<C, F extends Failure, R> extends IO<C, F, R> {
        final ThrowingSupplier<R, Throwable> supplier;

        public EffectPartial(ThrowingSupplier<R, Throwable> supplier) {
            tag = Tag.EffectPartial;
            this.supplier = supplier;
        }
    }

    static class Blocking<C, F, R>
        extends IO<C, F, R>
    {
        IO<C, F, R> io;

        public Blocking(
            IO<C, F, R> io
        ) {
            tag = Tag.Blocking;
            this.io = io;
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

    static class Fork<C, F, R>
        extends IO<C, F, Fiber<F, R>>
    {
        IO<C, F, R> io;

        public Fork(
            IO<C, F, R> io
        ) {
            tag = Tag.Fork;
            this.io = io;
        }

        @Override
        public String toString() {
                return "Fork(" + io + ")";
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

        @Override
        public String toString() {
                return "FlatMap(" + io + ", " + fn + ")";
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
