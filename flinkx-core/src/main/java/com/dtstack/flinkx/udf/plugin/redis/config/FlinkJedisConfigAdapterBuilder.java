package com.dtstack.flinkx.udf.plugin.redis.config;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.Protocol;

import java.io.Serializable;

public class FlinkJedisConfigAdapterBuilder implements Serializable {

    private String host;
    private int port = Protocol.DEFAULT_PORT;
    private int timeout = Protocol.DEFAULT_TIMEOUT;
    private int database = Protocol.DEFAULT_DATABASE;
    private String password;
    private int maxTotal = GenericObjectPoolConfig.DEFAULT_MAX_TOTAL;
    private int maxIdle = GenericObjectPoolConfig.DEFAULT_MAX_IDLE;
    private int minIdle = GenericObjectPoolConfig.DEFAULT_MIN_IDLE;
    private int maxRedirections = 5;

    /**
     * Sets value for the {@code maxTotal} configuration attribute
     * for pools to be created with this configuration instance.
     *
     * @param maxTotal maxTotal the maximum number of objects that can be allocated by the pool, default value is 8
     * @return Builder itself
     */
    public FlinkJedisConfigAdapterBuilder setMaxTotal(int maxTotal) {
        this.maxTotal = maxTotal;
        return this;
    }

    /**
     * Sets value for the {@code maxIdle} configuration attribute
     * for pools to be created with this configuration instance.
     *
     * @param maxIdle the cap on the number of "idle" instances in the pool, default value is 8
     * @return Builder itself
     */
    public FlinkJedisConfigAdapterBuilder setMaxIdle(int maxIdle) {
        this.maxIdle = maxIdle;
        return this;
    }

    /**
     * Sets value for the {@code minIdle} configuration attribute
     * for pools to be created with this configuration instance.
     *
     * @param minIdle the minimum number of idle objects to maintain in the pool, default value is 0
     * @return Builder itself
     */
    public FlinkJedisConfigAdapterBuilder setMinIdle(int minIdle) {
        this.minIdle = minIdle;
        return this;
    }

    /**
     * Sets host.
     *
     * @param host host
     * @return Builder itself
     */
    public FlinkJedisConfigAdapterBuilder setHost(String host) {
        this.host = host;
        return this;
    }

    /**
     * Sets port.
     *
     * @param port port, default value is 6379
     * @return Builder itself
     */
    public FlinkJedisConfigAdapterBuilder setPort(int port) {
        this.port = port;
        return this;
    }

    /**
     * Sets timeout.
     *
     * @param timeout timeout, default value is 2000
     * @return Builder itself
     */
    public FlinkJedisConfigAdapterBuilder setTimeout(int timeout) {
        this.timeout = timeout;
        return this;
    }

    /**
     * Sets database index.
     *
     * @param database database index, default value is 0
     * @return Builder itself
     */
    public FlinkJedisConfigAdapterBuilder setDatabase(int database) {
        this.database = database;
        return this;
    }

    /**
     * Sets password.
     *
     * @param password password, if any
     * @return Builder itself
     */
    public FlinkJedisConfigAdapterBuilder setPassword(String password) {
        this.password = password;
        return this;
    }

    /**
     * Sets maxRedirections.
     *
     * @param maxRedirections
     * @return Builder itself
     */
    public FlinkJedisConfigAdapterBuilder setMaxRedirections(int maxRedirections) {
        this.maxRedirections = maxRedirections;
        return this;
    }


    /**
     * Builds JedisPoolConfig.
     *
     * @return JedisPoolConfig
     */
    public FlinkJedisConfigAdapter build() {
        return new FlinkJedisConfigAdapter(host, port, timeout, password, database, maxTotal, maxIdle, minIdle, maxRedirections);
    }

}
