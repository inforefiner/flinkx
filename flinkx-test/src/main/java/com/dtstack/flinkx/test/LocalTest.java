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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import com.dtstack.flink.api.java.MyLocalStreamEnvironment;
import com.dtstack.flinkx.binlog.reader.BinlogReader;
import com.dtstack.flinkx.carbondata.reader.CarbondataReader;
import com.dtstack.flinkx.carbondata.writer.CarbondataWriter;
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
import com.dtstack.flinkx.kudu.reader.KuduReader;
import com.dtstack.flinkx.kudu.writer.KuduWriter;
import com.dtstack.flinkx.localfs.reader.LocalfsReader;
import com.dtstack.flinkx.mongodb.reader.MongodbReader;
import com.dtstack.flinkx.mongodb.writer.MongodbWriter;
import com.dtstack.flinkx.mysql.reader.MysqlReader;
import com.dtstack.flinkx.mysql.writer.MysqlWriter;
import com.dtstack.flinkx.mysqld.reader.MysqldReader;
import com.dtstack.flinkx.odps.reader.OdpsReader;
import com.dtstack.flinkx.odps.writer.OdpsWriter;
import com.dtstack.flinkx.oracle.reader.OracleReader;
import com.dtstack.flinkx.oracle.writer.OracleWriter;
import com.dtstack.flinkx.phoenix.reader.PhoenixReader;
import com.dtstack.flinkx.phoenix.writer.PhoenixWriter;
import com.dtstack.flinkx.polardb.reader.PolardbReader;
import com.dtstack.flinkx.polardb.writer.PolardbWriter;
import com.dtstack.flinkx.postgresql.reader.PostgresqlReader;
import com.dtstack.flinkx.postgresql.writer.PostgresqlWriter;
import com.dtstack.flinkx.reader.BaseDataReader;
import com.dtstack.flinkx.redis.writer.RedisWriter;
import com.dtstack.flinkx.restapi.writer.RestapiWriter;
import com.dtstack.flinkx.sqlserver.reader.SqlserverReader;
import com.dtstack.flinkx.sqlserver.writer.SqlserverWriter;
import com.dtstack.flinkx.stream.reader.StreamReader;
import com.dtstack.flinkx.stream.writer.StreamWriter;
import com.dtstack.flinkx.streaming.runtime.partitioner.CustomPartitioner;
import com.dtstack.flinkx.udf.UserDefinedFunctionRegistry;
import com.dtstack.flinkx.util.ConvertUtil;
import com.dtstack.flinkx.util.ResultPrintUtil;
import com.dtstack.flinkx.writer.BaseDataWriter;
import com.dtstack.flinkx.writer.DataWriterFactory;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
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
    public static Configuration conf = new Configuration();

    public static void main(String[] args) throws Exception{
        setLogLevel(Level.INFO.toString());
        Properties confProperties = new Properties();
//        confProperties.put("flink.checkpoint.interval", "10000");
//        confProperties.put("flink.checkpoint.stateBackend", "file:///tmp/flinkx_checkpoint");

//        conf.setString("metrics.reporter.promgateway.class","org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporter");
//        conf.setString("metrics.reporter.promgateway.host","127.0.0.1");
//        conf.setString("metrics.reporter.promgateway.port","9091");
//        conf.setString("metrics.reporter.promgateway.jobName","108job");
//        conf.setString("metrics.reporter.promgateway.randomJobNameSuffix","true");
//        conf.setString("metrics.reporter.promgateway.deleteOnShutdown","true");


        String jobPath = "flinkx-test/src/main/resources/test_hdfs_read.json";
        String savePointPath = "";
        JobExecutionResult result = LocalTest.runJob(new File(jobPath), confProperties, savePointPath);

        ResultPrintUtil.printResult(result);
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

        List<ContentConfig> content = config.getJob().getContent();
        ContentConfig firstContent = content.get(0);
        List<ReaderConfig> readerConfigs = firstContent.getReader();
        for (ReaderConfig readerConfig : readerConfigs) {
            BaseDataReader reader = buildDataReader(config, readerConfig, env);
            List column = readerConfig.getParameter().getColumn();
            RowTypeInfo rowTypeInfo = ConvertUtil.buildRowTypeInfo(column);
            DataStream<Row> dataStream = reader.readData();
            String[] fieldNames = rowTypeInfo.getFieldNames();
            SpeedConfig speedConfig = config.getJob().getSetting().getSpeed();
            if (speedConfig.getReaderChannel() > 0) {
                dataStream = ((DataStreamSource<Row>) dataStream).setParallelism(speedConfig.getReaderChannel());
            }

            /*
            * 处理后得到RowTypeInfo才能注册成Table
            * */
            SingleOutputStreamOperator<Row> rowDataStream = dataStream.map(row -> row).returns(rowTypeInfo);

            tableContext.registerDataStream(readerConfig.getStreamName(), rowDataStream,
                    org.apache.commons.lang3.StringUtils.join(fieldNames, ","));
        }

        TransformationConfig transformation = firstContent.getTransformationConfig();
        String sql = transformation.getSql();
        LOG.info("processing sql: {}", sql);


        Table table = tableContext.sqlQuery(sql);
        System.out.println("==================");
        table.printSchema();
        System.out.println("==================");

        //数据处理
        DataStream<Row> dataStream1 = tableContext.toAppendStream(table, Row.class);

        //多writer构造
        List<WriterConfig> writerConfigs = firstContent.getWriter();
        for (WriterConfig writerConfig : writerConfigs) {
            BaseDataWriter dataWriter = DataWriterFactory.getDataWriter(config, writerConfig);

            SpeedConfig speedConfig = config.getJob().getSetting().getSpeed();
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
            case PluginNameConstants.PHOENIX_READER : reader = new PhoenixReader(config, readerConfig, env); break;
            case PluginNameConstants.EMQX_READER : reader = new EmqxReader(config, readerConfig, env); break;
            case PluginNameConstants.DM_READER : reader = new DmReader(config, readerConfig, env); break;
            case PluginNameConstants.GREENPLUM_READER : reader = new GreenplumReader(config, readerConfig, env); break;
            case PluginNameConstants.LOCALFS_READER : reader = new LocalfsReader(config, readerConfig, env); break;
            case PluginNameConstants.KINGBASE_READER : reader = new KingbaseReader(config, readerConfig, env); break;
            default:throw new IllegalArgumentException("Can not find reader by name:" + readerName);
        }

        return reader;
    }

    private static void openCheckpointConf(StreamExecutionEnvironment env, Properties properties){
        if(properties == null){
            return;
        }

        if(properties.getProperty(ConfigConstant.FLINK_CHECKPOINT_INTERVAL_KEY) == null){
            return;
        }else{
            long interval = Long.parseLong(properties.getProperty(ConfigConstant.FLINK_CHECKPOINT_INTERVAL_KEY).trim());

            //start checkpoint every ${interval}
            env.enableCheckpointing(interval);

            LOG.info("Open checkpoint with interval:" + interval);
        }

        String checkpointTimeoutStr = properties.getProperty(ConfigConstant.FLINK_CHECKPOINT_TIMEOUT_KEY);
        if(checkpointTimeoutStr != null){
            long checkpointTimeout = Long.parseLong(checkpointTimeoutStr);
            //checkpoints have to complete within one min,or are discard
            env.getCheckpointConfig().setCheckpointTimeout(checkpointTimeout);

            LOG.info("Set checkpoint timeout:" + checkpointTimeout);
        }

        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        env.setStateBackend(new FsStateBackend(new Path("file:///tmp/flinkx_checkpoint")));
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                FAILURE_RATE,
                Time.of(FAILURE_INTERVAL, TimeUnit.MINUTES),
                Time.of(DELAY_INTERVAL, TimeUnit.SECONDS)
        ));
    }

    private static void setLogLevel(String level) {
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        ch.qos.logback.classic.Logger logger = loggerContext.getLogger("root");
        logger.setLevel(Level.toLevel(level));
    }
}
