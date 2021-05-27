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

package com.dtstack.flinkx.oscar.reader;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.inputformat.BaseRichInputFormat;
import com.dtstack.flinkx.oscar.OscarDatabaseMeta;
import com.dtstack.flinkx.oscar.format.OscarInputFormat;
import com.dtstack.flinkx.rdb.datareader.JdbcDataReader;
import com.dtstack.flinkx.rdb.datareader.QuerySqlBuilder;
import com.dtstack.flinkx.rdb.inputformat.JdbcInputFormatBuilder;
import com.dtstack.flinkx.rdb.util.DbUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.Map;

/**
 * @author whj
 */
public class OscarReader extends JdbcDataReader {

    public OscarReader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);
        //更新table名称为schema.table
        String jdbcUrl = dbUrl;
        if(jdbcUrl.indexOf("?") > -1 && jdbcUrl.substring(jdbcUrl.indexOf("?")+1).contains("schema=")){
            String parameterStr = jdbcUrl.substring(jdbcUrl.indexOf("?")+1);
            String[] params = parameterStr.split("&");
            for(String str : params){
                String[] k_v = str.split("=");
                if(k_v != null && k_v.length == 2 && k_v[0].equals("schema")){
                    String schemaParam = k_v[1];
                    table = schemaParam + "." +table;
                }
            }
        }
        //重新配置jdbcUrl
        Map<String,String> paramMap = new HashMap();
        paramMap.put("zeroDateTimeBehavior", "convertToNull");
        paramMap.put("serverTimezone", "UTC");
        dbUrl = DbUtil.formatJdbcUrl(dbUrl, paramMap);

        setDatabaseInterface(new OscarDatabaseMeta());

    }

    @Override
    protected JdbcInputFormatBuilder getBuilder() {
        return new JdbcInputFormatBuilder(new OscarInputFormat());
    }

    @Override
    public DataStream<Row> readData() {
        JdbcInputFormatBuilder builder = new JdbcInputFormatBuilder(new OscarInputFormat());
        builder.setDatabaseInterface(databaseInterface);
        builder.setDriverName(databaseInterface.getDriverClass());
        builder.setDbUrl(dbUrl);
        builder.setUsername(username);
        builder.setPassword(password);
        builder.setBytes(bytes);
        builder.setMonitorUrls(monitorUrls);
        builder.setTable(table);
        builder.setDataTransferConfig(dataTransferConfig);
        builder.setTypeConverter(typeConverter);
        builder.setMetaColumn(metaColumns);
        builder.setFetchSize(fetchSize == 0 ? databaseInterface.getFetchSize() : fetchSize);
        builder.setQueryTimeOut(queryTimeOut == 0 ? databaseInterface.getQueryTimeout() : queryTimeOut);
        builder.setIncrementConfig(incrementConfig);
        builder.setSplitKey(splitKey);
        builder.setNumPartitions(numPartitions);
        builder.setCustomSql(customSql);
        builder.setRestoreConfig(restoreConfig);
        builder.setHadoopConfig(hadoopConfig);
        builder.setTestConfig(testConfig);
        builder.setLogConfig(logConfig);

        QuerySqlBuilder sqlBuilder = new QuerySqlBuilder(this);
        builder.setQuery(sqlBuilder.buildSql());

        BaseRichInputFormat format =  builder.finish();
//        (databaseInterface.getDatabaseType() + "reader").toLowerCase()
        return createInput(format);
    }
}
