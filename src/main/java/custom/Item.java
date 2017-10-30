package custom;

import java.util.Map;

public class Item {

    private String primaryKey;

    private String tableName;

    private Map<String, Object> data;

    public Item(String primaryKey, Map<String, Object> data, String tableName) {
        this.primaryKey = primaryKey;
        this.data = data;
        this.tableName = tableName;
    }

    public String getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(String primaryKey) {
        this.primaryKey = primaryKey;
    }

    public Map<String, Object> getData() {
        return data;
    }

    public void setData(Map<String, Object> data) {
        this.data = data;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
}
