package fp.io;

import java.util.Objects;

import fp.util.Either;
import fp.util.ExceptionFailure;
import fp.util.Failure;
import fp.util.GeneralFailure;
import fp.util.Left;
import fp.util.Right;

public class Cause<F> {
    private final F value;
    private final Failure failure;
    private final Kind kind;

    private Cause(F value) {
        this.failure = GeneralFailure.of(value);;
        this.kind = Kind.Fail;
        this.value = value;
    }

    private Cause(Failure failure, Kind cause) {
        this.failure = failure;
        this.kind = cause;
        this.value = null;
    }

    public enum Kind {
        Die,
        Fail,
        Interrupt
    }

    /**
     * @return the failure
     */
    public Failure getFailure() {
        return failure;
    }

    /**
     * @return the failure
     */
    public F getValue() {
        return value;
    }

    /**
     * @return the cause
     */
    public Kind getCause() {
        return kind;
    }

    public static <F> Cause<F> fail(F failureValue) {
        return new Cause<F>(failureValue);
    }

    public static <F> Cause<F> die(ExceptionFailure failure) {
        if (failure.throwable instanceof InterruptedException) {
            return interrupt();
        }
        return new Cause<F>(failure, Kind.Die);
    }

    public static <F> Cause<F> die(Throwable throwable) {
        return new Cause<F>(ExceptionFailure.of(throwable), Kind.Die);
    }

    public static <F> Cause<F> interrupt() {
        return new Cause<F>(GeneralFailure.of("Interrupt"), Kind.Interrupt);
    }

    public boolean isDie() {
        return kind == Kind.Die;
    }

    public boolean isFail() {
        return kind == Kind.Fail;
    }

    public boolean isInterrupt() {
        return kind == Kind.Interrupt;
    }

    @Override
    public String toString() {
        switch (kind) {
            case Fail: return "Fail(" + value + ")";
            default:
                return "Exit(" + failure + ", " + kind + ")";
        }
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof Cause) {
            @SuppressWarnings("unchecked")
            Cause<F> cause = (Cause<F>) other;
            return Objects.equals(failure, cause.failure)
                && Objects.equals(kind, cause.kind);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return failure.hashCode() * 11 + kind.hashCode();
    }

    public static <F, R> Either<Failure, R> resultFlatten(
        Either<Cause<Failure>, R> result
    ) {
        return result.fold(
            cause -> Left.of(cause.getValue()),
            success -> Right.of(success)
        );
    }
}
