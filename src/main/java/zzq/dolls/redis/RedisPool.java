package zzq.dolls.redis;

import redis.clients.jedis.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class RedisPool {
    private String urls;

    private String authToken;

    private RedisMode redisMode;

    private int timeout;

    private int maxTotal;

    private int maxIdle;

    private String masterName;

    private JedisPool pool;
    private JedisSentinelPool sentinel;
    private JedisCluster cluster;

    private RedisPool(Builder builder) {
        this(builder.urls, builder.authToken, builder.redisMode, builder.timeout, builder.maxTotal, builder.maxIdle, builder.masterName);
    }

    public RedisPool(String urls, String authToken, RedisMode redisMode, int timeout, int maxTotal, int maxIdle, String masterName) {
        this.urls = urls;
        this.authToken = authToken;
        this.redisMode = redisMode;
        this.timeout = timeout;
        this.maxTotal = maxTotal;
        this.maxIdle = maxIdle;
        this.masterName = masterName;
        init();
    }

    private void init() {
        if (redisMode == RedisMode.STANDALONE) {
            String[] s = urls.split(":");
            HostAndPort hap = new HostAndPort(s[0], Integer.parseInt(s[1]));
            JedisPoolConfig c = new JedisPoolConfig();
            c.setMaxTotal(maxTotal);
            c.setMaxIdle(maxIdle);
            if (authToken != null)
                pool = new JedisPool(c, hap.getHost(), hap.getPort(), timeout, authToken);
            else
                pool = new JedisPool(c, hap.getHost(), hap.getPort(), timeout);
        } else if (redisMode == RedisMode.SENTINEL) {
            Set<String> sentinels = Arrays.stream(urls.split(";")).collect(Collectors.toSet());
            JedisPoolConfig c = new JedisPoolConfig();
            c.setMaxTotal(maxTotal);
            c.setMaxIdle(maxIdle);
            if (authToken != null)
                sentinel = new JedisSentinelPool(masterName, sentinels, c, timeout, authToken);
            else
                sentinel = new JedisSentinelPool(masterName, sentinels, c, timeout);
        } else if (redisMode == RedisMode.CLUSTER) {
            Set<HostAndPort> set = Arrays.stream(urls.split(";")).map(it -> {
                String[] hps = it.split(":");
                return new HostAndPort(hps[0], Integer.parseInt(hps[1]));
            }).collect(Collectors.toSet());
            JedisPoolConfig c = new JedisPoolConfig();
            c.setMaxTotal(maxTotal);
            c.setMaxIdle(maxIdle);
            cluster = new JedisCluster(set, c);
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

    public static final class Builder {

        private String urls = null;

        private String authToken = null;

        private RedisMode redisMode = RedisMode.STANDALONE;

        private int timeout = 30 * 1000;

        private int maxTotal = 8;

        private int maxIdle = 8;

        private String masterName = "nomaster";

        public RedisPool build() {
            return new RedisPool(this);
        }

        /**
         * 设置redis服务地址
         * STANDALONE -> ip:port
         * SENTINEL, CLUSTER -> ip:port;ip:port
         * @param urls redis地址 HostAndPort
         * @return
         */
        public Builder urls(String urls) {
            this.urls = urls;
            return this;
        }

        /**
         * 设置服务口令，可选
         * @param authToken 口令
         * @return
         */
        public Builder authToken(String authToken) {
            this.authToken = authToken;
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
    }
}
