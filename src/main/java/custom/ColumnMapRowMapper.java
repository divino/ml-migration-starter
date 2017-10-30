package custom;

import org.springframework.jdbc.support.JdbcUtils;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Map;

public class ColumnMapRowMapper extends org.springframework.jdbc.core.ColumnMapRowMapper {

    public static String PK_MAP_KEY  = "private#pk";
    public static String NAME_MAP_KEY = "private#name";

    private String name;
    private String primaryKey;

    public ColumnMapRowMapper(String name, String primaryKey) {
        this.name = name;
        this.primaryKey = primaryKey;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(String primaryKey) {
        this.primaryKey = primaryKey;
    }

    @Override
    public Map<String, Object> mapRow(ResultSet rs, int rowNum) throws SQLException {
        ResultSetMetaData rsmd = rs.getMetaData();
        int columnCount = rsmd.getColumnCount();
        Map<String, Object> mapOfColValues = createColumnMap(columnCount);
        for (int i = 1; i <= columnCount; i++) {
            String key = getColumnKey(JdbcUtils.lookupColumnName(rsmd, i));
            Object obj = getColumnValue(rs, i);
            mapOfColValues.put(key, obj);
        }
        mapOfColValues.put(PK_MAP_KEY, this.primaryKey);
        mapOfColValues.put(NAME_MAP_KEY, this.name);
        return mapOfColValues;
    }

}
