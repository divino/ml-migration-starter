package custom;

import org.springframework.jdbc.support.JdbcUtils;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class ColumnMapRowMapper extends org.springframework.jdbc.core.ColumnMapRowMapper {

    public static String PRIVATE_MAP_KEY_PREFIX  = "private#";
    public static String PK_MAP_KEY  = PRIVATE_MAP_KEY_PREFIX + "pk";
    public static String NAME_MAP_KEY = PRIVATE_MAP_KEY_PREFIX + "name";
    public static String METADATA_MAP_KEY  = PRIVATE_MAP_KEY_PREFIX + "metadata";

    private static Map<String, String> metadata = null;

    private String name;
    private String primaryKey;

    public ColumnMapRowMapper(String name, String primaryKey) {
        this.name = name;
        this.primaryKey = primaryKey;
    }

    @Override
    public Map<String, Object> mapRow(ResultSet rs, int rowNum) throws SQLException {
        ResultSetMetaData rsmd = rs.getMetaData();
        int columnCount = rsmd.getColumnCount();
        Map<String, Object> mapOfColValues = createColumnMap(columnCount);
        Map<String, String> mapMeta = null;
        for (int i = 1; i <= columnCount; i++) {
            String key = getColumnKey(JdbcUtils.lookupColumnName(rsmd, i));
            Object obj = getColumnValue(rs, i);
            mapOfColValues.put(key, obj);
            if (metadata == null) {
                if (mapMeta == null) {
                    mapMeta = new HashMap<>();
                }
                mapMeta.put(key, rsmd.getColumnTypeName(i));
            }
        }
        if (metadata == null) {
            metadata = mapMeta;
        }
        mapOfColValues.put(PK_MAP_KEY, this.primaryKey);
        mapOfColValues.put(NAME_MAP_KEY, this.name);
        mapOfColValues.put(METADATA_MAP_KEY, metadata);
        return mapOfColValues;
    }

}
