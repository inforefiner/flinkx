package com.dtstack.flinkx.udf.udtf;

import com.dtstack.flinkx.config.DimensionConfig;
import com.dtstack.flinkx.reader.MetaColumn;
import com.dtstack.flinkx.udf.plugin.redis.RedisUtil;
import com.dtstack.flinkx.udf.plugin.redis.config.FlinkJedisConfigAdapter;
import com.dtstack.flinkx.udf.plugin.redis.config.FlinkJedisConfigAdapterBuilder;
import com.dtstack.flinkx.udf.plugin.redis.config.FlinkJedisConfigBase;
import com.dtstack.flinkx.udf.plugin.redis.container.RedisCommandsContainer;
import com.dtstack.flinkx.udf.plugin.redis.container.RedisCommandsContainerBuilder;
import com.dtstack.flinkx.udf.util.ClassUtil;
import com.dtstack.flinkx.udf.util.RecordUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.*;

/**
 * Created by P0007 on 2019/9/9.
 */
@Slf4j
public class RedisLookup extends TableFunction<Row> {

    private String additionalKey;
    private String[] keyColumns;
    private int[] keyIndexes;

    private FlinkJedisConfigAdapter jedisConfigAdapter;
    private FlinkJedisConfigAdapterBuilder jedisConfigAdapterBuilder;
    private RedisCommandsContainer redisCommandsContainer;

    private RowTypeInfo rowTypeInfo;
    private String[] filedTypes;
    private final Row row;

    public RedisLookup(DimensionConfig dimensionConfig) {
        String keyColumn = dimensionConfig.getParameter().getStringVal("keyColumn");
        String url = dimensionConfig.getParameter().getStringVal("url");
        String _password = dimensionConfig.getParameter().getStringVal("password");
        String password = StringUtils.isEmpty(_password) ? null : _password;
        String table = dimensionConfig.getParameter().getStringVal("table");

        int database = dimensionConfig.getParameter().getIntVal("database", 0);
        int timeout =  dimensionConfig.getParameter().getIntVal("timeout", 2000);
        int maxTotal = dimensionConfig.getParameter().getIntVal("maxTotal", 8);
        int maxIdle =  dimensionConfig.getParameter().getIntVal("maxIdle", 8);
        int minIdle =  dimensionConfig.getParameter().getIntVal("minIdle", 0);
        int maxRedirections = dimensionConfig.getParameter().getIntVal("maxRedirections", 5);

        List columnList = dimensionConfig.getParameter().getColumn();
        List<MetaColumn> metaColumns = MetaColumn.getMetaColumns(columnList, false);
        String[] fieldNames = metaColumns.stream().map(metaColumn -> metaColumn.getName()).toArray(String[]::new);
        String[] fieldTypes = metaColumns.stream().map(metaColumn -> metaColumn.getType()).toArray(String[]::new);

        String[] keyCols = keyColumn.split(",");

        //Jedis conf
        String[] hostPort = url.split(":");

        FlinkJedisConfigAdapterBuilder builder = new FlinkJedisConfigAdapterBuilder()
                .setHost(hostPort[0])
                .setPort(Integer.parseInt(hostPort[1]))
                .setPassword(password)
                .setDatabase(database)
                .setTimeout(timeout)
                .setMaxTotal(maxTotal)
                .setMaxIdle(maxIdle)
                .setMinIdle(minIdle)
                .setMaxRedirections(maxRedirections);

        // constructor
        this.jedisConfigAdapterBuilder = builder;
        this.additionalKey = table;
        this.filedTypes = fieldTypes;
        this.rowTypeInfo = RecordUtil.buildTypeInfo(ClassUtil.toTypeInformations(fieldTypes), fieldNames);
        this.keyColumns = keyCols;
        this.keyIndexes = new int[keyCols.length];
        for (int i = 0; i < keyCols.length; i++) {
            keyIndexes[i] = rowTypeInfo.getFieldIndex(keyCols[i]);
        }
        row = new Row(fieldNames.length);
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        try {
            this.jedisConfigAdapter = jedisConfigAdapterBuilder.build();
            FlinkJedisConfigBase flinkJedisConfig = jedisConfigAdapter.getFlinkJedisConfig();
            RedisCommandsContainer redisCommandsContainer = RedisCommandsContainerBuilder.build(flinkJedisConfig);
            this.redisCommandsContainer = redisCommandsContainer;
            this.redisCommandsContainer.open();
        } catch (Exception e) {
            log.error("Redis has not been properly initialized: ", e);
            throw e;
        }
    }

    public void eval(String key) {
        String rediesKey = RedisUtil.rediesKey(additionalKey, key);
        Map<String, String> value = redisCommandsContainer.hgetAll(rediesKey);
        String[] fieldNames = rowTypeInfo.getFieldNames();
        for (int i = 0; i < fieldNames.length; i++) {
            String filedType = filedTypes[i];
            String valueStr;
            if (value != null && !value.isEmpty()) {
                if (ArrayUtils.contains(keyIndexes, i)) {
                    valueStr = key;
                } else {
                    valueStr = value.get(fieldNames[i]);
                }
            } else {
                valueStr = null;
            }
            Object convertedValue = ClassUtil.convert(valueStr, filedType);
            row.setField(i, convertedValue);
        }
        log.debug("Lookup key {}, collect row {}", key, row);
        collect(row);
    }


    /**
     * nokia定制开发UDF，在查询redis的同时，更新redis数据
     * @param key redis key
     * @param columnKeyStr 需要设置的字段,多个字段按:分割
     * @param columnValueStr 需要设置的字段值,同样按:分割
     * @param ttl 过期时间(毫秒)
     */
    public void eval(String key, String columnKeyStr, String columnValueStr, int ttl) {
        String rediesKey = RedisUtil.rediesKey(additionalKey, key);
        Map<String, String> value = redisCommandsContainer.hgetAll(rediesKey);
        String[] fieldNames = rowTypeInfo.getFieldNames();
        for (int i = 0; i < fieldNames.length; i++) {
            String filedType = filedTypes[i];
            String valueStr;
            if (value != null && !value.isEmpty()) {
                if (ArrayUtils.contains(keyIndexes, i)) {
                    valueStr = key;
                } else {
                    valueStr = value.get(fieldNames[i]);
                }
            } else {
                valueStr = null;
            }
            Object convertedValue = ClassUtil.convert(valueStr, filedType);
            row.setField(i, convertedValue);
        }
        log.debug("Lookup key {}, collect row {}", key, row);
        collect(row);

        String[] columns = columnKeyStr.split(":");
        String[] values = columnValueStr.split(":");
        Map<String, String> result = new HashMap<>();
        for (int i = 0; i < columns.length; i++) {
            int index = rowTypeInfo.getFieldIndex(columns[i]);
            String type = rowTypeInfo.getTypeAt(index).toString().toLowerCase();
            Object cacheValue = row.getField(index);
            String setValue = values[i];
            Object updateValue = null;
            if ("int".equals(type)) {
                Integer v1 = (Integer) cacheValue;
                Integer v2 = Integer.parseInt(setValue);
                updateValue = v1 + v2;
            } else if ("long".equals(type)) {
                Long v1 = (Long) cacheValue;
                Long v2 = Long.parseLong(setValue);
                updateValue = v1 + v2;
            } else if ("float".equals(type)) {
                Float v1 = (Float) cacheValue;
                Float v2 = Float.parseFloat(setValue);
                updateValue = v1 + v2;
            } else if ("double".equals(type)) {
                Double v1 = (Double) cacheValue;
                Double v2 = Double.parseDouble(setValue);
                updateValue = v1 + v2;
            } else if ("string".equals(type)) {
                String v1 = (String) cacheValue;
                String v2 = setValue;
                if (StringUtils.isEmpty(v1) && !StringUtils.isEmpty(v2)) {
                    updateValue = v2;
                } else if (!StringUtils.isEmpty(v1) && StringUtils.isEmpty(v2)) {
                    updateValue = v1;
                } else if (!StringUtils.isEmpty(v1) && !StringUtils.isEmpty(v2)) {
                    updateValue = v1 + "," + v2;
                }
            }
            result.put(columns[i], String.valueOf(updateValue));
        }
        redisCommandsContainer.hsetex(rediesKey, ttl, result);
    }

    /**
     * 模糊查询redies的key，返回所有符合keyPattern的map值
     *
     * 注意：需要保证redis dataset keyColumn指定的顺序，组合key以','分割。如：id,name
     *
     * @param keyPattern redie模糊查找key
     * @param tableSeperator 表名分隔符
     * @param keyColumnSeperator 组合key的key分隔符
     */
    public void eval(String keyPattern, String tableSeperator, String keyColumnSeperator) {
        String rediesKeyPattern = RedisUtil.rediesKey(additionalKey, keyPattern);
        Set<String> keys = redisCommandsContainer.keys(rediesKeyPattern);
        List<Map<String, String>> valueMapList = redisCommandsContainer.hgetAllWithBatch(keys);
        String[] fieldNames = rowTypeInfo.getFieldNames();
        Iterator<String> iterator = keys.iterator();
        int valueMapIndex = 0;
        while (iterator.hasNext()) {
            String keyCombinationWithTable = iterator.next();
            int tableIndexOf = keyCombinationWithTable.indexOf(tableSeperator) + 1;
            String keyCombination = keyCombinationWithTable.substring(tableIndexOf);
            String[] keyCols = keyCombination.split(keyColumnSeperator);
            int j = 0;
            for (int i = 0; i < fieldNames.length; i++) {
                String filedType = filedTypes[i];
                String valueStr;
                if (ArrayUtils.contains(keyIndexes, i)) {
                    valueStr = keyCols[j++];
                } else {
                    Map<String, String> valueMap = valueMapList.get(valueMapIndex);
                    valueStr = valueMap.get(fieldNames[i]);
                }
                Object convertedValue = ClassUtil.convert(valueStr, filedType);
                row.setField(i, convertedValue);
            }
            valueMapIndex++;
            log.debug("Lookup without compare keyPattern  {}, collect row {}", keyPattern, row);
            collect(row);
        }
    }

    /**
     * NOKIA定制，查找当前地点是否是过去两个小时去过的地点
     * 模糊查询redies的key，返回所有符合keyPattern的map值
     *
     * 注意：需要保证redis dataset keyColumn指定的顺序，组合key以','分割。如：id,name
     *
     * @param keyPattern redie模糊查找key
     * @param address 当前用户地点
     * @param tableSeperator 表名分隔符
     * @param keyColumnSeperator 组合key的key分隔符
     */
    public void eval(String keyPattern, String tableSeperator, String keyColumnSeperator, String compareCol, String address) {
        String rediesKeyPattern = RedisUtil.rediesKey(additionalKey, keyPattern);
        Set<String> keys = redisCommandsContainer.keys(rediesKeyPattern);
        List<Map<String, String>> valueMapList = redisCommandsContainer.hgetAllWithBatch(keys);
        String[] fieldNames = rowTypeInfo.getFieldNames();
        int compareColIndex = rowTypeInfo.getFieldIndex(compareCol);
        Iterator<String> iterator = keys.iterator();
        int valueMapIndex = 0;
        List<Row> resultRows = new ArrayList<>();
        boolean often = false;//常去地
        while (iterator.hasNext()) {
            String keyCombinationWithTable = iterator.next();
            int tableIndexOf = keyCombinationWithTable.indexOf(tableSeperator) + 1;
            String keyCombination = keyCombinationWithTable.substring(tableIndexOf);
            String[] keyCols = keyCombination.split(keyColumnSeperator);
            Row row = new Row(fieldNames.length);
            int j = 0;
            for (int i = 0; i < fieldNames.length; i++) {
                String filedType = filedTypes[i];
                String valueStr;
                if (ArrayUtils.contains(keyIndexes, i)) {
                    valueStr = keyCols[j++];
                } else {
                    Map<String, String> valueMap = valueMapList.get(valueMapIndex);
                    valueStr = valueMap.get(fieldNames[i]);
                }
                Object convertedValue = ClassUtil.convert(valueStr, filedType);
                //常去地判断
                if (i == compareColIndex && address.compareTo(convertedValue.toString()) == 0) {
                    often = true;
                }
                row.setField(i, convertedValue);
            }
            valueMapIndex++;
            log.debug("Lookup keyPattern {}, collect row {}", keyPattern, row);
            resultRows.add(row);
        }

        //emit result
        if (!often) {
            for (Row row : resultRows) {
                collect(row);
            }
        }
    }

    @Override
    public void close() throws Exception {
        if (redisCommandsContainer != null) {
            redisCommandsContainer.close();
        }
    }

    @Override
    public TypeInformation<Row> getResultType() {
        return rowTypeInfo;
    }
}