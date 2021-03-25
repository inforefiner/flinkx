package com.dtstack.flinkx.rdb.util;

import com.dtstack.flinkx.util.ExceptionUtil;
import lombok.extern.slf4j.Slf4j;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class TypeUtil {

    /**
     * 获取结果集的列类型信息
     *
     * @param resultSet 查询结果集
     * @return 字段类型list列表
     */
    public static List<Integer> analyzeColumn(ResultSet resultSet) {
        List<Integer> columnTypeList = new ArrayList<>();
        try {
            ResultSetMetaData rd = resultSet.getMetaData();
            for (int i = 1; i <= rd.getColumnCount(); i++) {
                columnTypeList.add(rd.getColumnType(i));
            }
        } catch (SQLException e) {
            log.error("error to analyzeSchema, resultSet = {}, e = {}", resultSet, ExceptionUtil.getErrorMessage(e));
            throw new RuntimeException(e);
        }
        return columnTypeList;
    }

    public static boolean isBinaryType(int sqlType) {
        switch (sqlType) {
            case Types.BLOB:
            case Types.BINARY:
            case Types.LONGVARBINARY:
            case Types.VARBINARY:
                return true;
            default:
                return false;
        }
    }

    public static boolean isClobType(int sqlType) {
        switch (sqlType) {
            case Types.CLOB:
                return true;
            default:
                return false;
        }
    }

    public static boolean isNclobType(int sqlType) {
        switch (sqlType) {
            case Types.NCLOB:
                return true;
            default:
                return false;
        }
    }

}
