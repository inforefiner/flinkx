/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flinkx.postgresql.format;

import com.dtstack.flinkx.rdb.inputformat.JdbcInputFormat;
import com.dtstack.flinkx.rdb.util.DbUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.types.Row;
import org.postgresql.util.PGobject;

import java.io.IOException;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

/**
 * Date: 2019/09/20 Company: www.dtstack.com
 *
 * @author tudou
 */
public class PostgresqlInputFormat extends JdbcInputFormat {
    private static final long serialVersionUID = 4711729902155186458L;
    private List<Integer> columnTypeList;

    private static final List<String> OBJECT_TYPES = Arrays.asList(//
        "point", "line", "lseg", "box", "path", "polygon", "circle", // 几何类型
        "cidr", "inet", "macaddr", "macaddr8", // 网路类型
        "int4range", "int8range", "tsrange", "tstzrange", "daterange", "numrange", // 范围类型
        "json", "jsonb", "jsonpath", // json类型
        "int2vector", "oidvector", // vector类型
        "tsvector", "tsquery", // 文本搜索类型
        "uuid", // UUID类型
        "cid", "interval", "sys_lsn", "txid_snapshot"//
    );

    @Override
    public void openInternal(InputSplit inputSplit) throws IOException {
        // 避免result.next阻塞
        if (incrementConfig.isPolling() && StringUtils.isEmpty(incrementConfig.getStartLocation())
            && fetchSize == databaseInterface.getFetchSize()) {
            fetchSize = 1000;
        }
        super.openInternal(inputSplit);
        columnTypeList = DbUtil.analyzeColumn(resultSet);
    }

    @Override
    public Row nextRecordInternal(Row row) throws IOException {
        if (!hasNext) {
            return null;
        }
        row = new Row(columnCount);
        try {
            for (int pos = 0; pos < row.getArity(); pos++) {
                Object obj = resultSet.getObject(pos + 1);
                if (obj != null) {
                    if (DbUtil.isBinaryType(columnTypeList.get(pos))) {
                        obj = resultSet.getBytes(pos + 1);
                    } else if (DbUtil.isClobType(columnTypeList.get(pos))) {
                        Clob clob = resultSet.getClob(pos + 1);
                        obj = clob.getSubString(1, (int) clob.length());
                    } else if (DbUtil.isNclobType(columnTypeList.get(pos))) {
                        NClob nClob = resultSet.getNClob(pos + 1);
                        obj = nClob.getSubString(1, (int) nClob.length());
                    } else if (DbUtil.isTimezoneType(columnTypeList.get(pos))) {
                        Timestamp timestamp = resultSet.getTimestamp(pos + 1);
                        obj = timestamp.toString();
                    } else if (DbUtil.isTimeType(columnTypeList.get(pos))) {
                        obj = resultSet.getTimestamp(pos + 1);
                    } else if (CollectionUtils.isNotEmpty(descColumnTypeList)) {
                        String columnType = descColumnTypeList.get(pos);
                        if ("xml".equalsIgnoreCase(columnType) || "xmltype".equalsIgnoreCase(columnType)) {
                            obj = String.valueOf(obj).replace("\r\n", "").replace("\n", "");
                        } else if ("tinyint".equalsIgnoreCase(columnType) || "bit".equalsIgnoreCase(columnType)) {
                            if (obj instanceof Boolean) {
                                obj = ((Boolean) obj ? 1 : 0);
                            }
                        } else if ("timestamptz".equalsIgnoreCase(columnType)) {
                            Timestamp timestamp = resultSet.getTimestamp(pos + 1);
                            obj = String.valueOf(timestamp);
                        } else if ("time".equalsIgnoreCase(columnType)) { // JdbcUtils中将Time转化为了timestamp(getCatalystType)
                            obj = resultSet.getTimestamp(pos + 1);
                        } else if ("int8".equalsIgnoreCase(columnType)) {
                            obj = resultSet.getLong(pos + 1);
                        } else if (OBJECT_TYPES.contains(columnType.toLowerCase())) {
                            if (obj instanceof PGobject) {
                                obj = ((PGobject) obj).getValue();
                            }
                        }
                    }
                }
                row.setField(pos, obj);
            }
            return super.nextRecordInternal(row);
        } catch (Exception e) {
            throw new IOException("Couldn't read data - " + e.getMessage(), e);
        }
    }
}
