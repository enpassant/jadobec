package fp.util;

import java.util.Optional;

public interface Failure {
    public static <E extends Exception, R> Optional<R> tryCatchOptional(
        SupplierCatch<E, R> process
    ) {
        try {
            return Optional.of(process.get());
        } catch(Exception e) {
            return ignoreException(e, Optional.empty());
        }
    }

    public static <E extends Exception, R> R ignoreException(E e, R r) {
        e.getCause();
        return r;
    }

    @FunctionalInterface
    public static interface SupplierCatch<E extends Exception, R> {
        R get() throws E;
    }
}
