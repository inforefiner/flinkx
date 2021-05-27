package com.dtstack.flinkx.oscar;

import com.dtstack.flinkx.enums.EDatabaseType;
import com.dtstack.flinkx.rdb.BaseDatabaseMeta;

import java.util.List;

/**
* @author whj
* @Date 15:13 2021/5/25
*/
public class OscarDatabaseMeta extends BaseDatabaseMeta {

    @Override
    protected String makeValues(List<String> column) {
        StringBuilder sb = new StringBuilder("SELECT ");
        for (int i = 0; i < column.size(); ++i) {
            if (i != 0) {
                sb.append(",");
            }
            sb.append("? " + quoteColumn(column.get(i)));
        }
        sb.append(" FROM DUAL");
        return sb.toString();
    }

    @Override
    public String quoteTable(String table) {
        table = table.replace("\"", "");
        return table;
    }

    @Override
    public EDatabaseType getDatabaseType() {
        return EDatabaseType.Oscar;
    }

    @Override
    public String getDriverClass() {
        return "com.oscar.Driver";
    }

    @Override
    public String getSqlQueryFields(String tableName) {
        return "SELECT * FROM " + tableName + " LIMIT 1";
    }

    @Override
    public String getSqlQueryColumnFields(List<String> column, String table) {
        return "SELECT " + quoteColumns(column) + " FROM " + quoteTable(table) + " LIMIT 1";
    }

    @Override
    public String quoteValue(String value, String column) {
        return String.format("'%s' as %s", value, column);
    }

    @Override
    public String getSplitFilter(String columnName) {
        return String.format("mod(%s,${N}) = ${M}", getStartQuote() + columnName + getEndQuote());
    }

    @Override
    public String getSplitFilterWithTmpTable(String tmpTable, String columnName) {
        return String.format("mod(%s.%s,${N}) = ${M}", tmpTable, getStartQuote() + columnName + getEndQuote());
    }

    @Override
    public int getFetchSize() {
        return 1000;
    }

    @Override
    public int getQueryTimeout() {
        return 3000;
    }
}
