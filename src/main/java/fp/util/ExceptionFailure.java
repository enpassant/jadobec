package fp.util;

import fp.jadobec.ThrowingConsumer;
import fp.jadobec.ThrowingFunction;
import fp.jadobec.ThrowingSupplier;

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
    
    public static <E extends Exception, F, R>
    	Either<Failure, R> tryCatchFinal
    (
    	ThrowingSupplier<F, E> supplier,
        ThrowingFunction<F, R, E> function,
        ThrowingConsumer<F, E> finalConsumer
    ) {
    	F resource = null;
        try {
        	resource = supplier.get();
            return Right.of(function.apply(resource));
        } catch(Exception e) {
            return Left.of(
                ExceptionFailure.of(e)
            );
        } finally {
        	try {
        		if (resource != null) {
        			finalConsumer.accept(resource);
        		}
        	} catch(Exception e2) {      		
        	}
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
