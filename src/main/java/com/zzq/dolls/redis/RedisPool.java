package com.zzq.dolls.redis;

import redis.clients.jedis.*;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class RedisPool {
    private Set<String> urls;

    private String password;

    private RedisMode redisMode;

    private int timeout;

    private int maxTotal;

    private int maxIdle;

    private String masterName;

    private int db;

    private JedisPool pool;
    private JedisSentinelPool sentinel;
    private JedisCluster cluster;

    private RedisPool(Builder builder) {
        this(builder.urls, builder.password, builder.redisMode, builder.timeout, builder.maxTotal, builder.maxIdle, builder.masterName, builder.db);
    }

    public RedisPool(Set<String> urls, String password, RedisMode redisMode, int timeout, int maxTotal, int maxIdle, String masterName, int db) {
        this.urls = urls;
        this.password = password;
        this.redisMode = redisMode;
        this.timeout = timeout;
        this.maxTotal = maxTotal;
        this.maxIdle = maxIdle;
        this.masterName = masterName;
        this.db = db;
        init();
    }

    private void init() {
        if (urls == null) {
            throw new NullPointerException("urls is null? try call function Builder.urls()");
        }
        if (redisMode == RedisMode.STANDALONE) {
            if (urls.size() > 1) {
                throw new UnsupportedOperationException("urls are incorrect, redis mode is RedisMode.STANDALONE?");
            }
            for (String url : urls) {
                String[] s = url.split(":");
                HostAndPort hap = new HostAndPort(s[0], Integer.parseInt(s[1]));
                JedisPoolConfig config = new JedisPoolConfig();
                config.setMaxTotal(maxTotal);
                config.setMaxIdle(maxIdle);
                pool = new JedisPool(config, hap.getHost(), hap.getPort(), timeout, password, db);
            }
        }
        else if (redisMode == RedisMode.SENTINEL) {
            if (masterName == null) {
                throw new UnsupportedOperationException("master name can't null");
            }
            JedisPoolConfig config = new JedisPoolConfig();
            config.setMaxTotal(maxTotal);
            config.setMaxIdle(maxIdle);
            sentinel = new JedisSentinelPool(masterName, urls, config, timeout, password, db);
        }
        else if (redisMode == RedisMode.CLUSTER) {
            Set<HostAndPort> set = urls.stream().map(it -> {
                String[] hps = it.split(":");
                return new HostAndPort(hps[0], Integer.parseInt(hps[1]));
            }).collect(Collectors.toSet());
            JedisPoolConfig config = new JedisPoolConfig();
            config.setMaxTotal(maxTotal);
            config.setMaxIdle(maxIdle);
            cluster = new JedisCluster(set, timeout, timeout, 10, password, config);
        }
    }

    /**
     * set redisMode -> RedisMode.STANDALONE or RedisMode.SENTINEL
     * @param r redis接口
     * @param <T> 返回值类型
     * @return T
     */
    public <T> T jedis(Function<Jedis, T> r) {
        if (redisMode != RedisMode.STANDALONE && redisMode != RedisMode.SENTINEL)
            throw new IllegalThreadStateException("redis mode is not standalone or sentinel");
        try (Jedis jedis = getResource()) {
            if (jedis != null) {
                return r.apply(jedis);
            } else {
                return null;
            }
        }
    }

    ///**
    // * set redisMode -> RedisMode.STANDALONE or RedisMode.SENTINEL
    // * @param r redis接口
    // */
    //public void jedis(Consumer<Jedis> r) {
    //    if (redisMode != RedisMode.STANDALONE && redisMode != RedisMode.SENTINEL)
    //        throw new IllegalThreadStateException("redis mode is not standalone or sentinel");
    //    try (Jedis jedis = getResource()) {
    //        if (jedis != null) {
    //            r.accept(jedis);
    //        }
    //    }
    //}

    /**
     * set redisMode -> RedisMode.CLUSTER
     * @param r cluster接口
     * @param <T> 返回值类型
     * @return T
     */
    public <T> T cluster(Function<JedisCluster, T> r) {
        if (redisMode != RedisMode.CLUSTER)
            throw new IllegalThreadStateException("redis mode is not cluster");
        return r.apply(cluster);
    }

    ///**
    // * set redisMode -> RedisMode.CLUSTER
    // * @param r cluster接口
    // */
    //public void cluster(Consumer<JedisCluster> r) {
    //    if (redisMode != RedisMode.CLUSTER)
    //        throw new IllegalThreadStateException("redis mode is not cluster");
    //    r.accept(cluster);
    //}

    public void close() throws IOException {
        if (redisMode == RedisMode.STANDALONE) {
            pool.close();
        } else if (redisMode == RedisMode.SENTINEL) {
            sentinel.close();
        } else if (redisMode == RedisMode.CLUSTER) {
            cluster.close();
        }
    }

    private Jedis getResource() {
        if (redisMode == RedisMode.STANDALONE) {
            return pool.getResource();
        } else if (redisMode == RedisMode.SENTINEL) {
            return sentinel.getResource();
        } else {
            return null;
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {

        private Set<String> urls = null;

        private String password = null;

        private RedisMode redisMode = RedisMode.STANDALONE;

        private int timeout = 30 * 1000;

        private int maxTotal = 8;

        private int maxIdle = 8;

        private String masterName = "nomaster";

        private int db = 0;

        public RedisPool build() {
            return new RedisPool(this);
        }

        public Builder url(final String url) {
            if (this.urls == null) {
                this.urls = new HashSet<String>();
            }
            this.urls.add(url);
            return this;
        }

        /**
         * 设置redis服务地址
         * @param urls redis地址 HostAndPort
         * @return
         */
        public Builder urls(Collection<String> urls) {
            if (this.urls == null) {
                this.urls = new HashSet<String>();
            }
            this.urls.addAll(urls);
            return this;
        }

        /**
         * 设置服务口令，可选
         * @param password 口令
         * @return
         */
        public Builder password(String password) {
            this.password = password;
            return this;
        }

        /**
         * 设置redis模式
         * @see RedisMode
         * @param redisMode redis服务模式， 默认 STANDALONE
         * @return
         */
        public Builder redisMode(RedisMode redisMode) {
            this.redisMode = redisMode;
            return this;
        }

        /**
         * 设置redis模式
         * @see RedisMode
         * @param redisMode redis服务模式， 默认 STANDALONE
         * @return
         */
        public Builder redisMode(int redisMode) {
            this.redisMode = RedisMode.create(redisMode);
            return this;
        }

        /**
         * 设置redis超时时间
         * @param timeout redis超时时间， 默认 30s
         * @return
         */
        public Builder timeout(int timeout) {
            this.timeout = timeout;
            return this;
        }

        /**
         * 设置redis最大连接数
         * @param maxTotal 最大连接数, 默认8个
         * @return
         */
        public Builder maxTotal(int maxTotal) {
            this.maxTotal = maxTotal;
            return this;
        }

        /**
         * 设置redis最大空闲连接数
         * @param maxIdle 最大空闲连接数, 默认8个
         * @return
         */
        public Builder maxIdle(int maxIdle) {
            this.maxIdle = maxIdle;
            return this;
        }

        /**
         * STANDALONE下，设置master name
         * @param masterName standalone master name
         * @return
         */
        public Builder masterName(String masterName) {
            this.masterName = masterName;
            return this;
        }

        /**
         * 设置database  默认0
         * @param db database id
         * @return
         */
        public Builder db(int db) {
            this.db = db;
            return this;
        }
    }
}
