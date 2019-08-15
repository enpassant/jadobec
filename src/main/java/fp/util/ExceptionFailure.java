package fp.util;

public class ExceptionFailure implements Failure {
	private final Exception exception;

	private ExceptionFailure(Exception exception) {
        this.exception = exception;
    }

    public static ExceptionFailure of(Exception exception) {
        return new ExceptionFailure(exception);
    }
    
    public static <E extends Exception, R> Either<Failure, R> tryCatch(
        SupplierCatch<E, R> process
    ) {
        try {
            return Right.of(process.get());
        } catch(Exception e) {
            return Left.of(
                ExceptionFailure.of(e)
            );
        }
    }

    @Override
    public String toString() {
        return "ExceptionFailure(" + exception.toString() + ")";
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof ExceptionFailure) {
            ExceptionFailure failure = (ExceptionFailure) other;
            return failure.toString().equals(this.toString());
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return exception.hashCode();
    }
}
