package custom;

import custom.util.MetadataReaderUtil;
import org.springframework.jdbc.core.ColumnMapRowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

public class RowToColumnMapWithMetadataMapper extends ColumnMapRowMapper {

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
        Map<String, Object> mapOfColValues = super.mapRow(rs, rowNum);
        if (mapOfColValues.size() == 0) {
            return null;
        } else {
            mapOfColValues.put(MetadataReaderUtil.META_MAP_KEY, metadata);
            return mapOfColValues;
        }
    }

}
