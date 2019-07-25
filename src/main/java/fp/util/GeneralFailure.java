package fp.util;

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class GeneralFailure implements Failure {
    public static final String EXCEPTION = "EXCEPTION";
    protected final String code;
    protected final Map<String, Object> params;

    private GeneralFailure(String code, Map<String, Object> params) {
        this.code = code;
        this.params = params;
    }

    public static GeneralFailure of(String code) {
        return new GeneralFailure(code, new HashMap<>());
    }

    public static GeneralFailure of(String code, String key, Object value) {
        Map<String, Object> params = new HashMap<>();
        params.put(key, value);
        return new GeneralFailure(code, params);
    }

    public static GeneralFailure of(Exception e) {
        return GeneralFailure.of(e.getClass().getSimpleName(), EXCEPTION, e);
    }

    @SafeVarargs
    public static GeneralFailure of(String code, Tuple2<String, Object>... tuples) {
        return new GeneralFailure(code, Tuple2.toMap(tuples));
    }
    
    public static <E extends Exception, R> Either<Failure, R> tryCatch(
        SupplierCatch<E, R> process
    ) {
        try {
            return Right.of(process.get());
        } catch(Exception e) {
            return Left.of(
                GeneralFailure.of(e)
            );
        }
    }

    public static <E extends Exception, R> Either<Failure, R> tryCatch(
        String code,
        SupplierCatch<E, R> process
    ) {
        try {
            return Right.of(process.get());
        } catch(Exception e) {
            return Left.of(
                GeneralFailure.of(code, EXCEPTION, e)
            );
        }
    }

    public String getCode() {
        return code;
    }

    public Set<String> getParamNames() {
        return params.keySet();
    }

    @SuppressWarnings("unchecked")
    public <T> T getParamValue(String paramName) {
        return (T) params.get(paramName);
    }

    public String format(String pattern) {
        return MessageFormat.format(
            pattern,
            params.values().toArray(new Object[0])
        );
    }

    @Override
    public String toString() {
        final Optional<String> paramStrOpt = params.entrySet()
            .stream()
            .map(entry -> entry.getKey() + " -> " + entry.getValue())
            .reduce((s1, s2) -> s1 + ", " + s2);
        final String paramStr = paramStrOpt.map(p -> ", " + p).orElse("");

        return "GeneralFailure(" + code + paramStr + ")";
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof GeneralFailure) {
            GeneralFailure failure = (GeneralFailure) other;
            return failure.toString().equals(this.toString());
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return code.hashCode();
    }
}
