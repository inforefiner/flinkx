package com.dtstack.flinkx.udf.plugin.redis.config;

import com.dtstack.flinkx.udf.plugin.redis.container.RedisCommandsContainer;
import com.dtstack.flinkx.udf.plugin.redis.container.RedisCommandsContainerBuilder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.Set;

/**
 * Created by P0007 on 2020/3/30.
 */
@Slf4j
public class FlinkJedisConfigAdapter implements Serializable {

    private FlinkJedisPoolConfig jedisPoolConfig;

    private RedisCommandsContainer container;

    @Getter
    private String host;

    @Getter
    private int port;

    @Getter
    private int connectionTimeout;

    @Getter
    private String password;

    @Getter
    private int database;

    @Getter
    private int maxTotal;

    @Getter
    private int maxIdle;

    @Getter
    private int minIdle;

    @Getter
    private int maxRedirections;

    public FlinkJedisConfigAdapter(String host, int port, int connectionTimeout, String password, int database,
                                   int maxTotal, int maxIdle, int minIdle, int maxRedirections) {
       this.host = host;
       this.port = port;
       this.connectionTimeout = connectionTimeout;
       this.password = password;
       this.database = database;
       this.maxTotal = maxTotal;
       this.maxIdle = maxIdle;
       this.minIdle = minIdle;
       this.maxRedirections = maxRedirections;

        FlinkJedisPoolConfig jedisPoolConfig = initJedisPoolConfig(host, port, connectionTimeout, password, database, maxTotal, maxIdle, minIdle);
        RedisCommandsContainer container = RedisCommandsContainerBuilder.build(jedisPoolConfig);
        this.container = container;
        this.jedisPoolConfig = jedisPoolConfig;
    }

    private boolean isClusterEnabled() {
        return container.isClusterEnabled();
    }

    private Set<InetSocketAddress> getNodes() {
        return container.getNodes();
    }

    /**
     * 支持自动从单点配置转换到集群配置
     * @return
     */
    public FlinkJedisConfigBase getFlinkJedisConfig() {
        boolean clusterEnabled = isClusterEnabled();
        if (clusterEnabled) {
            Set<InetSocketAddress> nodes = getNodes();
            FlinkJedisClusterConfig jedisClusterConfig = initJedisClusterConfig(nodes);
            return jedisClusterConfig;
        } else {
            return jedisPoolConfig;
        }
    }

    /**
     * 初始化 jedis cluster connect pool
     * @param nodes
     * @return
     */
    private FlinkJedisClusterConfig initJedisClusterConfig(Set<InetSocketAddress> nodes) {
        return new FlinkJedisClusterConfig.Builder()
                        .setNodes(nodes)
                        .setTimeout(connectionTimeout)
                        .setMaxTotal(maxTotal)
                        .setMinIdle(minIdle)
                        .setMaxIdle(maxIdle)
                        .setMaxRedirections(maxRedirections)
                        .setPassword(password)
                        .build();
    }

    /**
     * 初始化 jedis单点connect pool
     * @param host
     * @param port
     * @param connectionTimeout
     * @param password
     * @param database
     * @param maxTotal
     * @param maxIdle
     * @param minIdle
     * @return
     */
    private FlinkJedisPoolConfig initJedisPoolConfig(String host, int port, int connectionTimeout, String password, int database, int maxTotal, int maxIdle, int minIdle) {
        return new FlinkJedisPoolConfig.Builder()
                .setHost(host)
                .setPort(port)
                .setPassword(password)
                .setMaxIdle(maxIdle)
                .setMinIdle(minIdle)
                .setDatabase(database)
                .setMaxTotal(maxTotal)
                .setTimeout(connectionTimeout)
                .build();
    }


}
