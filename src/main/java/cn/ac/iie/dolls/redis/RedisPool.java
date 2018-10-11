package cn.ac.iie.dolls.redis;

import redis.clients.jedis.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
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

    public RedisPool(Builder builder) {
        this(builder.urls, builder.authToken, builder.redisMode, builder.timeout, builder.maxTotal, builder.maxIdle, builder.masterName);
    }

    private RedisPool(String urls, String authToken, RedisMode redisMode, int timeout, int maxTotal, int maxIdle, String masterName) {
        this.urls = urls;
        this.authToken = authToken;
        this.redisMode = redisMode;
        this.timeout = timeout;
        this.maxTotal = maxTotal;
        this.maxIdle = maxIdle;
        this.masterName = masterName;
        init();
    }

    private RedisPool init() {
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
        return this;
    }

    public <T> T handler(Handler<T> r) {
        try (Jedis jedis = getResource()) {
            if (jedis != null) {
                return r.apply(jedis);
            } else {
                return null;
            }
        }
    }

    public <T> T cluster(Cluster<T> r) {
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

        private RedisMode redisMode;

        private int timeout = 30 * 1000;

        private int maxTotal = 8;

        private int maxIdle = 8;

        private String masterName = "nomaster";

        public RedisPool build() {
            return new RedisPool(this);
        }

        public Builder urls(String urls) {
            this.urls = urls;
            return this;
        }

        public Builder authToken(String authToken) {
            this.authToken = authToken;
            return this;
        }

        public Builder redisMode(RedisMode redisMode) {
            this.redisMode = redisMode;
            return this;
        }

        public Builder timeout(int timeout) {
            this.timeout = timeout;
            return this;
        }

        public Builder maxTotal(int maxTotal) {
            this.maxTotal = maxTotal;
            return this;
        }

        public Builder maxIdle(int maxIdle) {
            this.maxIdle = maxIdle;
            return this;
        }

        public Builder masterName(String masterName) {
            this.masterName = masterName;
            return this;
        }
    }
}
