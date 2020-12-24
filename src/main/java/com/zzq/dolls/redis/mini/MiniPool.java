package com.zzq.dolls.redis.mini;

import com.zzq.dolls.redis.RedisMode;
import com.zzq.dolls.redis.module.RedisModule;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.util.Pool;

import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class MiniPool {

    private RedisMode redisMode;

    private Set<String> urls;

    private String password;

    private int timeout;

    private int maxTotal;

    private int maxIdle;

    private String masterName;

    private int db;

    protected Pool<JedisMini> pool;
    protected JedisClusterMini cluster;

    public MiniPool(Set<String> urls, String password, RedisMode redisMode,
                     int timeout, int maxTotal, int maxIdle, String masterName,
                     int db) {
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

    protected void init() {
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
                pool = new JedisPoolMini(config, hap.getHost(), hap.getPort(), timeout, password, db);
            }
        }
        else if (redisMode == RedisMode.SENTINEL) {
            if (masterName == null) {
                throw new UnsupportedOperationException("master name can't null");
            }
            JedisPoolConfig config = new JedisPoolConfig();
            config.setMaxTotal(maxTotal);
            config.setMaxIdle(maxIdle);
            pool = new JedisSentinelPoolMini(masterName, urls, config, timeout, password, db);
        }
        else if (redisMode == RedisMode.CLUSTER) {
            Set<HostAndPort> set = urls.stream().map(it -> {
                String[] hps = it.split(":");
                return new HostAndPort(hps[0], Integer.parseInt(hps[1]));
            }).collect(Collectors.toSet());
            JedisPoolConfig config = new JedisPoolConfig();
            config.setMaxTotal(maxTotal);
            config.setMaxIdle(maxIdle);
            cluster = new JedisClusterMini(set, timeout, timeout, 10, password, config);
        }
    }

    /**
     * set redisMode -> RedisMode.STANDALONE or RedisMode.SENTINEL
     * @param r redis接口
     * @param <T> 返回值类型
     * @return T
     */
    public <T> T jedis(Function<Redis, T> r) {
        if (redisMode == RedisMode.STANDALONE || redisMode == RedisMode.SENTINEL) {
            try (JedisMini jedis = getResource()) {
                if (jedis != null) {
                    return r.apply(jedis);
                } else {
                    return null;
                }
            }
        } else if (redisMode == RedisMode.CLUSTER) {
            return r.apply(cluster);
        } else {
            throw new RuntimeException("redis mode is error!");
        }

    }

    private JedisMini getResource() {
        if (redisMode == RedisMode.STANDALONE || redisMode == RedisMode.SENTINEL) {
            return pool.getResource();
        } else {
            return null;
        }
    }

}
