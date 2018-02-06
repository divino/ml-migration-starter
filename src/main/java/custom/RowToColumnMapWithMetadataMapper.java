package custom;

import custom.util.MetadataReaderUtil;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class RowToColumnMapWithMetadataMapper extends org.springframework.jdbc.core.ColumnMapRowMapper {

    private Map<Integer, String> order;
    private Map<String, Object> metadata;
    private Integer colCount;

    public RowToColumnMapWithMetadataMapper(Map<String, Object> metadata) {
        this.order = (Map<Integer, String>) metadata.get(MetadataReaderUtil.ORDER_MAP_KEY);
        this.colCount = order.keySet().size();
        this.metadata = metadata;
    }

    @Override
    public Map<String, Object> mapRow(ResultSet rs, int rowNum) throws SQLException {
        Map<String, Object> mapOfColValues = new HashMap<>();
        while (rs.next()) {
            for (int i = 1; i <= colCount ; i++) {
                mapOfColValues.put(order.get(i), getColumnValue(rs, i));
            }
            mapOfColValues.put(MetadataReaderUtil.META_MAP_KEY, metadata);
        }
        if (mapOfColValues.size() == 0) {
            return null;
        }
        return mapOfColValues;
    }

}
