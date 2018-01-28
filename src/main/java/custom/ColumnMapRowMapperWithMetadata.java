package custom;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class ColumnMapRowMapperWithMetadata extends org.springframework.jdbc.core.ColumnMapRowMapper {

    private Map<Integer, String> order;
    private Map<String, Object> metadata = new HashMap<>();
    private Integer colCount;

    public ColumnMapRowMapperWithMetadata(Map<String, Object> metadata) {
        //System.out.println(" ColumnMapRowMapperWithMetadata " + metadata.toString());
        this.order = (Map<Integer, String>) metadata.get(MetadataReader.ORDER_MAP_KEY);
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
            mapOfColValues.put(MetadataReader.META_MAP_KEY, metadata);
        }
        if (mapOfColValues.size() == 0) {
            return null;
        }
        //System.out.println(" mapRow " + metadata.toString());
        return mapOfColValues;
    }

}
