package fp.jadobec;

import java.sql.ResultSet;
import java.sql.SQLException;

@FunctionalInterface
public interface Extractor<T> {
    T extract(ResultSet rs) throws SQLException;
}
