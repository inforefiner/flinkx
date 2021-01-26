package com.dtstack.flinkx.config;

import java.util.Map;

public class TransformationConfig extends AbstractConfig {

    public static String KEY_SQL_CONFIG = "sql";

    private String sql;

    public TransformationConfig(Map<String, Object> map) {
        super(map);
        this.sql = getStringVal(KEY_SQL_CONFIG);
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }
}
