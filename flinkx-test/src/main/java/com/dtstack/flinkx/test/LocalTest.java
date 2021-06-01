/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.flinkx.test;

import com.dtstack.flink.api.java.MyLocalStreamEnvironment;
import com.dtstack.flinkx.binlog.reader.BinlogReader;
import com.dtstack.flinkx.carbondata.reader.CarbondataReader;
import com.dtstack.flinkx.carbondata.writer.CarbondataWriter;
import com.dtstack.flinkx.classloader.PluginUtil;
import com.dtstack.flinkx.clickhouse.reader.ClickhouseReader;
import com.dtstack.flinkx.clickhouse.writer.ClickhouseWriter;
import com.dtstack.flinkx.config.*;
import com.dtstack.flinkx.constants.ConfigConstant;
import com.dtstack.flinkx.db2.reader.Db2Reader;
import com.dtstack.flinkx.db2.writer.Db2Writer;
import com.dtstack.flinkx.dm.reader.DmReader;
import com.dtstack.flinkx.dm.writer.DmWriter;
import com.dtstack.flinkx.emqx.reader.EmqxReader;
import com.dtstack.flinkx.emqx.writer.EmqxWriter;
import com.dtstack.flinkx.es.reader.EsReader;
import com.dtstack.flinkx.es.writer.EsWriter;
import com.dtstack.flinkx.ftp.reader.FtpReader;
import com.dtstack.flinkx.ftp.writer.FtpWriter;
import com.dtstack.flinkx.gbase.reader.GbaseReader;
import com.dtstack.flinkx.gbase.writer.GbaseWriter;
import com.dtstack.flinkx.greenplum.reader.GreenplumReader;
import com.dtstack.flinkx.greenplum.writer.GreenplumWriter;
import com.dtstack.flinkx.hbase.reader.HbaseReader;
import com.dtstack.flinkx.hbase.writer.HbaseWriter;
import com.dtstack.flinkx.hdfs.reader.HdfsReader;
import com.dtstack.flinkx.hdfs.writer.HdfsWriter;
import com.dtstack.flinkx.hive.writer.HiveWriter;
import com.dtstack.flinkx.kafka.reader.KafkaReader;
import com.dtstack.flinkx.kafka.writer.KafkaWriter;
import com.dtstack.flinkx.kafka09.reader.Kafka09Reader;
import com.dtstack.flinkx.kafka09.writer.Kafka09Writer;
import com.dtstack.flinkx.kafka10.reader.Kafka10Reader;
import com.dtstack.flinkx.kafka10.writer.Kafka10Writer;
import com.dtstack.flinkx.kafka11.reader.Kafka11Reader;
import com.dtstack.flinkx.kafka11.writer.Kafka11Writer;
import com.dtstack.flinkx.kingbase.reader.KingbaseReader;
import com.dtstack.flinkx.kingbase.writer.KingbaseWriter;
import com.dtstack.flinkx.kudu.reader.KuduReader;
import com.dtstack.flinkx.kudu.writer.KuduWriter;
import com.dtstack.flinkx.mongodb.reader.MongodbReader;
import com.dtstack.flinkx.mongodb.writer.MongodbWriter;
import com.dtstack.flinkx.mysql.reader.MysqlReader;
import com.dtstack.flinkx.mysql.writer.MysqlWriter;
import com.dtstack.flinkx.mysqld.reader.MysqldReader;
import com.dtstack.flinkx.odps.reader.OdpsReader;
import com.dtstack.flinkx.odps.writer.OdpsWriter;
import com.dtstack.flinkx.oracle.reader.OracleReader;
import com.dtstack.flinkx.oracle.writer.OracleWriter;
import com.dtstack.flinkx.oraclelogminer.reader.OraclelogminerReader;
import com.dtstack.flinkx.phoenix5.reader.Phoenix5Reader;
import com.dtstack.flinkx.phoenix5.writer.Phoenix5Writer;
import com.dtstack.flinkx.polardb.reader.PolardbReader;
import com.dtstack.flinkx.polardb.writer.PolardbWriter;
import com.dtstack.flinkx.postgresql.reader.PostgresqlReader;
import com.dtstack.flinkx.postgresql.writer.PostgresqlWriter;
import com.dtstack.flinkx.reader.BaseDataReader;
import com.dtstack.flinkx.reader.DataReaderFactory;
import com.dtstack.flinkx.redis.writer.RedisWriter;
import com.dtstack.flinkx.restapi.reader.RestapiReader;
import com.dtstack.flinkx.sqlserver.reader.SqlserverReader;
import com.dtstack.flinkx.sqlserver.writer.SqlserverWriter;
import com.dtstack.flinkx.sqlservercdc.reader.SqlservercdcReader;
import com.dtstack.flinkx.stream.reader.StreamReader;
import com.dtstack.flinkx.stream.writer.StreamWriter;
import com.dtstack.flinkx.udf.UserDefinedFunctionRegistry;
import com.dtstack.flinkx.udf.udtf.JDBCLookup;
import com.dtstack.flinkx.udf.udtf.RedisLookup;
import com.dtstack.flinkx.util.ConvertUtil;
import com.dtstack.flinkx.util.ResultPrintUtil;
import com.dtstack.flinkx.writer.BaseDataWriter;
import com.dtstack.flinkx.writer.DataWriterFactory;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
/**
 * @author jiangbo
 */
public class LocalTest {

    private static final int FAILURE_RATE = 3;
    private static final int FAILURE_INTERVAL = 6;
    private static final int DELAY_INTERVAL = 10;
    public static Logger LOG = LoggerFactory.getLogger(LocalTest.class);
    public static Configuration conf = GlobalConfiguration.loadConfiguration("E:\\GitHub\\test\\flinkx\\flinkx-test\\src\\main\\resources");;

    public static void main(String[] args) throws Exception{
        Properties confProperties = new Properties();
        confProperties.put(ConfigConstant.FLINK_CHECKPOINT_INTERVAL_KEY, "5000");
        confProperties.put(ConfigConstant.FLINK_CHECKPOINT_TIMEOUT_KEY, "60000");
        confProperties.put(ConfigConstant.FLINK_CHECKPOINT_PATH_KEY, "file:///tmp/shiy/flinkx_checkpoint");

//        conf.setString("metrics.reporter.promgateway.class","org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporter");
//        conf.setString("metrics.reporter.promgateway.host","127.0.0.1");
//        conf.setString("metrics.reporter.promgateway.port","9091");
//        conf.setString("metrics.reporter.promgateway.jobName","108job");
//        conf.setString("metrics.reporter.promgateway.randomJobNameSuffix","true");
//        conf.setString("metrics.reporter.promgateway.deleteOnShutdown","true");

        String jobPath = "flinkx-test/src/main/resources/debug.json";
        JobExecutionResult result = LocalTest.runJob(new File(jobPath), confProperties, null);
        ResultPrintUtil.printResult(result);
        System.exit(0);
    }

    public static JobExecutionResult runJob(File jobFile, Properties confProperties, String savepointPath) throws Exception{
        String jobContent = readJob(jobFile);
        return runJob(jobContent, confProperties, savepointPath);
    }

    public static JobExecutionResult runJob(String job, Properties confProperties, String savepointPath) throws Exception{
        DataTransferConfig config = DataTransferConfig.parse(job);

        conf.setString("akka.ask.timeout", "180 s");
        conf.setString("web.timeout", String.valueOf(100000));

        MyLocalStreamEnvironment env = new MyLocalStreamEnvironment(conf);

        openCheckpointConf(env, confProperties);

        env.setParallelism(config.getJob().getSetting().getSpeed().getChannel());

        if (needRestart(config)) {
            env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                    10,
                    Time.of(10, TimeUnit.SECONDS)
            ));
        }

        //注册udf
        StreamTableEnvironment tableContext = StreamTableEnvironment.create(env);
        UserDefinedFunctionRegistry udfRegistry = new UserDefinedFunctionRegistry(tableContext);
        udfRegistry.registerInternalUDFs();

        Map<String, DataStream<Row>> readerDataStreams = new HashMap<>();
        List<ContentConfig> content = config.getJob().getContent();
        ContentConfig firstContent = content.get(0);

        SpeedConfig speedConfig = config.getJob().getSetting().getSpeed();
        PluginUtil.registerPluginUrlToCachedFile(config, firstContent, env);
        env.setParallelism(speedConfig.getChannel());

        List<ReaderConfig> readerConfigs = firstContent.getReader();
        for (ReaderConfig readerConfig : readerConfigs) {
            BaseDataReader dataReader = DataReaderFactory.getDataReader(config, readerConfig, env);
            DataStream<Row> dataStream = dataReader.readData();
            List column = readerConfig.getParameter().getColumn();
            RowTypeInfo rowTypeInfo = ConvertUtil.buildRowTypeInfo(column);
            String[] fieldNames = rowTypeInfo.getFieldNames();
            if (speedConfig.getReaderChannel() > 0) {
                dataStream = ((DataStreamSource<Row>) dataStream).setParallelism(speedConfig.getReaderChannel());
            }

            if (speedConfig.isRebalance()) {
                dataStream = dataStream.rebalance();
            }

            /*
             * 处理后得到RowTypeInfo才能注册成Table
             * */
            SingleOutputStreamOperator<Row> rowDataStream = dataStream.map(row -> row).returns(rowTypeInfo);

            //生成临时表
            String tableName = readerConfig.getTableName();
            String fieldsStr = org.apache.commons.lang3.StringUtils.join(fieldNames, ",");
            LOG.info("register table {} with files {}", tableName, fieldsStr);
            tableContext.registerDataStream(tableName, rowDataStream, fieldsStr);

            readerDataStreams.put(tableName, rowDataStream);
        }

        /* 维度表注册为UDTF */
        List<DimensionConfig> dimensionList = firstContent.getDimension();
        for (DimensionConfig dimension : dimensionList) {
            String dimensionName = dimension.getTableName();
            TableFunction<Row> tableFunction = buildDimension(config, dimension, env);
            LOG.info("register dimension table {}", dimensionName);
            tableContext.registerFunction(dimensionName, tableFunction);
        }

        TransformConfig transformConfig = firstContent.getTransformConfig();
        String sql = transformConfig.getSql();
        LOG.info("transform sql is:" + sql);

        //数据处理
        DataStream<Row> dataStream1 = tableContext.toAppendStream(tableContext.sqlQuery(sql), Row.class);

        //多writer构造
        List<WriterConfig> writerConfigs = firstContent.getWriter();
        for (WriterConfig writerConfig : writerConfigs) {
            BaseDataWriter dataWriter = DataWriterFactory.getDataWriter(config, writerConfig);

            //todo 获取sink datastream 输出
            DataStreamSink<?> dataStreamSink = dataWriter.writeData(dataStream1);
            if (speedConfig.getWriterChannel() > 0) {
                dataStreamSink.setParallelism(speedConfig.getWriterChannel());
            }
        }

        if(StringUtils.isNotEmpty(savepointPath)){
            env.setSettings(SavepointRestoreSettings.forPath(savepointPath));
        }

        return env.execute();
    }

    private static boolean needRestart(DataTransferConfig config){
        return config.getJob().getSetting().getRestoreConfig().isRestore();
    }

    private static String readJob(File file) {
        try(FileInputStream in = new FileInputStream(file)) {
            byte[] fileContent = new byte[(int) file.length()];
            in.read(fileContent);
            return new String(fileContent, StandardCharsets.UTF_8);
        } catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    private static BaseDataReader buildDataReader(DataTransferConfig config, ReaderConfig readerConfig, StreamExecutionEnvironment env){
        String readerName = readerConfig.getName();
        BaseDataReader reader ;
        switch (readerName){
            case PluginNameConstants.STREAM_READER : reader = new StreamReader(config, readerConfig, env); break;
            case PluginNameConstants.CARBONDATA_READER : reader = new CarbondataReader(config, readerConfig, env); break;
            case PluginNameConstants.ORACLE_READER : reader = new OracleReader(config, readerConfig, env); break;
            case PluginNameConstants.POSTGRESQL_READER : reader = new PostgresqlReader(config, readerConfig, env); break;
            case PluginNameConstants.SQLSERVER_READER : reader = new SqlserverReader(config, readerConfig, env); break;
            case PluginNameConstants.MYSQLD_READER : reader = new MysqldReader(config, readerConfig, env); break;
            case PluginNameConstants.MYSQL_READER : reader = new MysqlReader(config, readerConfig, env); break;
            case PluginNameConstants.DB2_READER : reader = new Db2Reader(config, readerConfig, env); break;
            case PluginNameConstants.GBASE_READER : reader = new GbaseReader(config, readerConfig, env); break;
            case PluginNameConstants.ES_READER : reader = new EsReader(config, readerConfig, env); break;
            case PluginNameConstants.FTP_READER : reader = new FtpReader(config, readerConfig, env); break;
            case PluginNameConstants.HBASE_READER : reader = new HbaseReader(config, readerConfig, env); break;
            case PluginNameConstants.HDFS_READER : reader = new HdfsReader(config, readerConfig, env); break;
            case PluginNameConstants.MONGODB_READER : reader = new MongodbReader(config, readerConfig, env); break;
            case PluginNameConstants.ODPS_READER : reader = new OdpsReader(config, readerConfig, env); break;
            case PluginNameConstants.BINLOG_READER : reader = new BinlogReader(config, readerConfig, env); break;
            case PluginNameConstants.KAFKA09_READER : reader = new Kafka09Reader(config, readerConfig, env); break;
            case PluginNameConstants.KAFKA10_READER : reader = new Kafka10Reader(config, readerConfig, env); break;
            case PluginNameConstants.KAFKA11_READER : reader = new Kafka11Reader(config, readerConfig, env); break;
            case PluginNameConstants.KAFKA_READER : reader = new KafkaReader(config, readerConfig, env); break;
            case PluginNameConstants.KUDU_READER : reader = new KuduReader(config, readerConfig, env); break;
            case PluginNameConstants.CLICKHOUSE_READER : reader = new ClickhouseReader(config, readerConfig, env); break;
            case PluginNameConstants.POLARDB_READER : reader = new PolardbReader(config, readerConfig, env); break;
            case PluginNameConstants.EMQX_READER : reader = new EmqxReader(config, readerConfig, env); break;
            case PluginNameConstants.DM_READER : reader = new DmReader(config, readerConfig, env); break;
            case PluginNameConstants.GREENPLUM_READER : reader = new GreenplumReader(config, readerConfig, env); break;
            case PluginNameConstants.PHOENIX5_READER : reader = new Phoenix5Reader(config, readerConfig, env); break;
            case PluginNameConstants.KINGBASE_READER : reader = new KingbaseReader(config, readerConfig, env); break;
            case PluginNameConstants.ORACLE_LOG_MINER_READER : reader = new OraclelogminerReader(config, readerConfig, env); break;
            case PluginNameConstants.RESTAPI_READER:  reader = new RestapiReader(config, readerConfig, env); break;
            case PluginNameConstants.SQLSERVER_CDC_READER:  reader = new SqlservercdcReader(config, readerConfig, env); break;
            default:throw new IllegalArgumentException("Can not find reader by name:" + readerName);
        }

        return reader;
    }

    private static BaseDataWriter buildDataWriter(DataTransferConfig config, WriterConfig writerConfig){
        String writerName = writerConfig.getName();
        BaseDataWriter writer;
        switch (writerName){
            case PluginNameConstants.STREAM_WRITER : writer = new StreamWriter(config, writerConfig); break;
            case PluginNameConstants.CARBONDATA_WRITER : writer = new CarbondataWriter(config, writerConfig); break;
            case PluginNameConstants.MYSQL_WRITER : writer = new MysqlWriter(config, writerConfig); break;
            case PluginNameConstants.SQLSERVER_WRITER : writer = new SqlserverWriter(config, writerConfig); break;
            case PluginNameConstants.ORACLE_WRITER : writer = new OracleWriter(config, writerConfig); break;
            case PluginNameConstants.POSTGRESQL_WRITER : writer = new PostgresqlWriter(config, writerConfig); break;
            case PluginNameConstants.DB2_WRITER : writer = new Db2Writer(config, writerConfig); break;
            case PluginNameConstants.GBASE_WRITER : writer = new GbaseWriter(config, writerConfig); break;
            case PluginNameConstants.ES_WRITER : writer = new EsWriter(config, writerConfig); break;
            case PluginNameConstants.FTP_WRITER : writer = new FtpWriter(config, writerConfig); break;
            case PluginNameConstants.HBASE_WRITER : writer = new HbaseWriter(config, writerConfig); break;
            case PluginNameConstants.HDFS_WRITER : writer = new HdfsWriter(config, writerConfig); break;
            case PluginNameConstants.MONGODB_WRITER : writer = new MongodbWriter(config, writerConfig); break;
            case PluginNameConstants.ODPS_WRITER : writer = new OdpsWriter(config, writerConfig); break;
            case PluginNameConstants.REDIS_WRITER : writer = new RedisWriter(config, writerConfig); break;
            case PluginNameConstants.HIVE_WRITER : writer = new HiveWriter(config, writerConfig); break;
            case PluginNameConstants.KAFKA09_WRITER : writer = new Kafka09Writer(config, writerConfig); break;
            case PluginNameConstants.KAFKA10_WRITER : writer = new Kafka10Writer(config, writerConfig); break;
            case PluginNameConstants.KAFKA11_WRITER : writer = new Kafka11Writer(config, writerConfig); break;
            case PluginNameConstants.KUDU_WRITER : writer = new KuduWriter(config, writerConfig); break;
            case PluginNameConstants.CLICKHOUSE_WRITER : writer = new ClickhouseWriter(config, writerConfig); break;
            case PluginNameConstants.POLARDB_WRITER : writer = new PolardbWriter(config, writerConfig); break;
            case PluginNameConstants.KAFKA_WRITER : writer = new KafkaWriter(config, writerConfig); break;
            case PluginNameConstants.EMQX_WRITER : writer = new EmqxWriter(config, writerConfig); break;
            case PluginNameConstants.DM_WRITER : writer = new DmWriter(config, writerConfig); break;
            case PluginNameConstants.GREENPLUM_WRITER : writer = new GreenplumWriter(config, writerConfig); break;
            case PluginNameConstants.PHOENIX5_WRITER : writer = new Phoenix5Writer(config, writerConfig); break;
            case PluginNameConstants.KINGBASE_WRITER : writer = new KingbaseWriter(config, writerConfig); break;
            case PluginNameConstants.RESTAPI_WRITER: writer = new RedisWriter(config, writerConfig); break;
            default:throw new IllegalArgumentException("Can not find writer by name:" + writerName);
        }

        return writer;
    }

    private static TableFunction<Row> buildDimension(DataTransferConfig config, DimensionConfig dimensionConfig, StreamExecutionEnvironment env){
        String type = dimensionConfig.getType();
        TableFunction<Row> dimensionFunction;
        switch (type.toLowerCase()){
            case "redis" : dimensionFunction = new RedisLookup(dimensionConfig); break;
            case "jdbc"  : dimensionFunction = new JDBCLookup(dimensionConfig); break;
            default:throw new IllegalArgumentException("Can not find dimension reader by type:" + type);
        }

        return dimensionFunction;
    }

    private static void openCheckpointConf(StreamExecutionEnvironment env, Properties properties){
        if(properties!=null){
            String interval = properties.getProperty(ConfigConstant.FLINK_CHECKPOINT_INTERVAL_KEY);
            if(StringUtils.isNotBlank(interval)){
                env.enableCheckpointing(Long.parseLong(interval.trim()));
                LOG.info("Open checkpoint with interval:" + interval);
            }
            String checkpointTimeoutStr = properties.getProperty(ConfigConstant.FLINK_CHECKPOINT_TIMEOUT_KEY);
            if(checkpointTimeoutStr != null){
                long checkpointTimeout = Long.parseLong(checkpointTimeoutStr.trim());
                //checkpoints have to complete within one min,or are discard
                env.getCheckpointConfig().setCheckpointTimeout(checkpointTimeout);

                LOG.info("Set checkpoint timeout:" + checkpointTimeout);
            }
            env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
            env.getCheckpointConfig().enableExternalizedCheckpoints(
                    CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

            String path = properties.getProperty(ConfigConstant.FLINK_CHECKPOINT_PATH_KEY);
            if (org.apache.commons.lang3.StringUtils.isNotBlank(path)) {
                LOG.info("Checkpoint data in {}", path);
                env.setStateBackend(new FsStateBackend(path, true));
            }
        }
    }
}
