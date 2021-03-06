package fp.jadobec;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

import fp.util.Either;
import fp.util.Failure;
import fp.util.ExceptionFailure;
import fp.util.Left;
import fp.util.Right;
import fp.util.ThrowingConsumer;

public class Record {
    private final Map<String, Object> values;

    private Record(Map<String, Object> values) {
        this.values = values;
    }

    @SuppressWarnings("unchecked")
    public <T> T fieldOrElse(String name, T elseValue) {
        return Optional.ofNullable((T) values.get(name))
            .orElse(elseValue);
    }

    public Optional<Object> field(String name) {
        return Optional.ofNullable(values.get(name));
    }

    public Set<String> fields() {
        return values.keySet();
    }

    public Collection<Object> values() {
        return values.values();
    }

    @SuppressWarnings("unchecked")
    public <T> Either<Failure, T> as(Class<T> type) {
        try {
            Constructor<?> constructors[] = type.getDeclaredConstructors();
            Constructor<?> ctRet = constructors[0];
            ctRet.setAccessible(true);
            Object arglist[] = values.values().toArray();
            return Right.of((T) ctRet.newInstance(arglist));
        } catch(Exception e) {
            return Left.of(
                ExceptionFailure.of(e)
            );
        }
    }

    public static <T> Either<Failure, T> ofAs(ResultSet rs, Class<T> type) {
        return of(rs).flatMap(record -> record.as(type));
    }

    public static <T> Extractor<Either<Failure, T>> expandAs(Class<T> type) {
        return rs -> ofAs(rs, type);
    }

    public Record copy(Consumer<Builder> factory) {
        final Builder builder = new Builder(values);
        factory.accept(builder);
        return builder.build();
    }

    public static Record build(Consumer<Builder> factory) {
        final Builder builder = new Builder();
        factory.accept(builder);
        return builder.build();
    }

    public static Either<Failure, Record> tryBuild(
        ThrowingConsumer<Builder, Exception> factory
    ) {
        final Builder builder = new Builder();
        return ExceptionFailure.tryCatch(() -> {
            factory.accept(builder);
            return builder.build();
        });
    }

    public static Either<Failure, Record> from(Object object) {
        return tryBuild(builder -> {
            final Class<?> type = object.getClass();
            final Field[] fields = type.getDeclaredFields();
            for (Field field : fields) {
                field.setAccessible(true);
                builder.field(
                    field.getName(),
                    field.get(object)
                );
            }
        });
    }

    public static Either<Failure, Record> of(ResultSet rs) {
        return ExceptionFailure.tryCatch(() -> {
            final Map<String, Object> values = new LinkedHashMap<>();
            final ResultSetMetaData rsmd = rs.getMetaData();
            final int numberOfColumns = rsmd.getColumnCount();

            for (int i=1; i<=numberOfColumns; i++) {
                values.put(
                    rsmd.getColumnLabel(i).toLowerCase(),
                    rs.getObject(i)
                );
            }

            return new Record(values);
        });
    }

    @Override
    public String toString() {
        final String fieldStr = values.entrySet()
            .stream()
            .map(entry -> entry.getKey() + " -> " + entry.getValue())
            .reduce((s1, s2) -> s1 + ", " + s2)
            .orElse("");

        return "Record(" + fieldStr + ")";
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof Record) {
            Record record = (Record) other;
            if (record.values.size() == values.size()) {
                for (String key : values.keySet()) {
                    if (!record.values.containsKey(key)
                       || !record.values.get(key).equals(values.get(key)))
                    {
                        return false;
                    }
                }
                return true;
            }
        }
        return false;
    }

    public static final class Builder {
        private final Map<String, Object> values;

        private Builder() {
            this.values = new LinkedHashMap<>();
        }

        private Builder(Map<String, Object> values) {
            this.values = values;
        }

        public Builder field(String name, Object value) {
            values.put(name, value);
            return this;
        }

        public <T, R> Builder modify(String name, Function<T, R> mapper) {
            try {
                if (values.containsKey(name)) {
                    @SuppressWarnings("unchecked")
                    final T value = (T) values.get(name);
                    final R result = mapper.apply(value);
                    values.put(name, result);
                }
            } catch(Exception e) {}

            return this;
        }

        private Record build() {
            return new Record(values);
        }
    }
}
