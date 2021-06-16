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
package com.dtstack.flinkx.db2.format;

import com.dtstack.flinkx.rdb.inputformat.JdbcInputFormat;
import com.dtstack.flinkx.rdb.util.DbUtil;
import com.dtstack.flinkx.util.CharsetUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.SQLException;
import java.util.List;

/**
 * Date: 2019/09/20 Company: www.dtstack.com
 *
 * @author tudou
 */
public class Db2InputFormat extends JdbcInputFormat {
    private static final long serialVersionUID = -2518261271913905942L;
    private List<Integer> columnTypeList;

    @Override
    public void openInternal(InputSplit inputSplit) throws IOException {
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
                    } else if (DbUtil.isTimeType(columnTypeList.get(pos))) {
                        obj = resultSet.getTimestamp(pos + 1);
                    } else if (obj instanceof BigDecimal) {
                        switch (metaColumns.get(pos).getType().toLowerCase()) {
                            case "int":
                                obj = resultSet.getInt(pos + 1);
                                break;
                            case "bigint":
                                obj = resultSet.getLong(pos + 1);
                                break;
                            case "short":
                                obj = resultSet.getShort(pos + 1);
                                break;
                            case "double":
                                obj = resultSet.getDouble(pos + 1);
                                break;
                            case "float":
                                obj = resultSet.getFloat(pos + 1);
                                break;
                        }
                    } else if (obj.getClass().getSimpleName().toUpperCase().contains("TIMESTAMP")) {
                        obj = resultSet.getTimestamp(pos + 1);
                    } else if (CollectionUtils.isNotEmpty(descColumnTypeList)) {
                        String columnType = descColumnTypeList.get(pos);
                        if ("xml".equalsIgnoreCase(columnType) || "xmltype".equalsIgnoreCase(columnType)) {
                            obj = CharsetUtils.toUTF_8(String.valueOf(obj)).replace("\r\n", "").replace("\n", "");
                        } else if ("time".equalsIgnoreCase(columnType)) {
                            obj = resultSet.getTimestamp(pos + 1);
                        } else if ((obj instanceof java.util.Date)) {
                            obj = resultSet.getDate(pos + 1);
                        } else if ("dbclob".equalsIgnoreCase(columnType)) {
                            Clob dbclob = resultSet.getClob(pos + 1);
                            obj = getUtf8StrFromUtf16beClob(dbclob);
                        } else if ("graphic".equalsIgnoreCase(columnType) || "vargraphic".equalsIgnoreCase(columnType)
                            || "long vargraphic".equalsIgnoreCase(columnType)) {
                            String vargraphic = resultSet.getString(pos + 1);
                            obj = CharsetUtils.toUTF_8(vargraphic);
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

    private static String getUtf8StrFromUtf16beClob(Clob c) throws IOException, SQLException {
        Reader reader = c.getCharacterStream();
        if (reader == null) {
            return null;
        }
        StringBuffer sb = new StringBuffer();
        char[] charbuf = new char[4096];
        for (int i = reader.read(charbuf); i > 0; i = reader.read(charbuf)) {
            sb.append(charbuf, 0, i);
        }
        return CharsetUtils.toUTF_8(sb.toString());
    }
}
