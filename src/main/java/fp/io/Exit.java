package fp.io;

import fp.util.Either;
import fp.util.ExceptionFailure;
import fp.util.Failure;
import fp.util.GeneralFailure;
import fp.util.Left;
import fp.util.Right;

public class Exit<F> {
    private final F value;
    private final Failure failure;
    private final Cause cause;
    
    private Exit(F value) {
        this.failure = GeneralFailure.of(value);;
        this.cause = Cause.Fail;
        this.value = value;
    }
    
    private Exit(Failure failure, Cause cause) {
        this.failure = failure;
        this.cause = cause;
        this.value = null;
    }

    public enum Cause {
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
    public Cause getCause() {
        return cause;
    }

    public static <F> Exit<F> fail(F failureValue) {
        return new Exit<F>(failureValue);
    }

    public static <F> Exit<F> die(ExceptionFailure failure) {
        return new Exit<F>(failure, Cause.Die);
    }

    public static <F> Exit<F> die(Throwable throwable) {
        return new Exit<F>(ExceptionFailure.of(throwable), Cause.Die);
    }

    public static <F> Exit<F> interrupt() {
        return new Exit<F>(GeneralFailure.of("Interrupt"), Cause.Interrupt);
    }

    public boolean isDie() {
        return cause == Cause.Die;
    }

    public boolean isFail() {
        return cause == Cause.Fail;
    }

    public boolean isInterrupt() {
        return cause == Cause.Interrupt;
    }

    @Override
    public String toString() {
        switch (cause) {
            case Fail: return "Fail(" + value + ")";
            default:
                return "Exit(" + failure + ", " + cause + ")";
        }
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof Exit) {
            @SuppressWarnings("unchecked")
            Exit<F> exit = (Exit<F>) other;
            return exit.toString().equals(this.toString());
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return failure.hashCode() * 11 + cause.hashCode();
    }
    
    public static <F, R> Either<Failure, R> resultFlatten(Either<Exit<Failure>, R> result) {
        return result.fold(
            exit -> Left.of(exit.getValue()),
            success -> Right.of(success)
        );
    }
}
