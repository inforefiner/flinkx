package com.dtstack.flinkx.config;

import java.util.List;
import java.util.Map;

public class DimensionConfig extends AbstractConfig {

    public static String KEY_PARAMETER_CONFIG = "parameter";

    public static final String KEY_READER_NAME = "name";

    public static final String KEY_TABLE_NAME = "tableName";

    public static final String KEY_TYPE_NAME = "type";

    private ParameterConfig parameter;

    public DimensionConfig(Map<String, Object> map) {
        super(map);
        parameter = new ParameterConfig((Map<String, Object>) getVal(KEY_PARAMETER_CONFIG));
    }

    public String getName() {
        return getStringVal(KEY_READER_NAME);
    }

    public void setName(String name) {
        setStringVal(KEY_READER_NAME, name);
    }

    public String getTableName() {
        return getParameter().getStringVal(KEY_TABLE_NAME);
    }

    public String getType() {
        return getStringVal(KEY_TYPE_NAME);
    }

    public void setType(String type) {
        setStringVal(KEY_TYPE_NAME, type);
    }

    public ParameterConfig getParameter() {
        return parameter;
    }

    public void setParameter(ParameterConfig parameter) {
        this.parameter = parameter;
    }

    public static class ParameterConfig extends AbstractConfig {
        public static final String KEY_COLUMN_LIST = "column";

        List column;

        public ParameterConfig(Map<String, Object> map) {
            super(map);
            column = (List) getVal(KEY_COLUMN_LIST);
        }

        public List getColumn() {
            return column;
        }

        public void setColumn(List column) {
            this.column = column;
        }

    }
}
