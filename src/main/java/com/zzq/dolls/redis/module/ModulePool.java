package com.zzq.dolls.redis.module;

import com.zzq.dolls.redis.RedisMode;
import com.zzq.dolls.redis.module.graph.RedisGraphs;
import com.zzq.dolls.redis.module.json.Json;
import com.zzq.dolls.redis.module.json.RedisJson;
import com.zzq.dolls.redis.module.json.RedisJsonCluster;
import com.zzq.dolls.redis.module.search.RedisSearch;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.util.Pool;

import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ModulePool {

    private RedisJson json;

    private RedisGraphs graph;

    private RedisSearch search;

    private RedisJsonCluster jsonCluster;

    private Set<RedisModule> redisModule;

    private RedisMode redisMode;

    public ModulePool(Pool<Jedis> pool, RedisMode redisMode, Set<RedisModule> redisModule) {
        this.redisMode = redisMode;
        this.redisModule = redisModule;
        if (redisMode != RedisMode.STANDALONE && redisMode != RedisMode.SENTINEL)
            throw new IllegalThreadStateException("redis mode is not standalone or sentinel");
        if (redisModule.contains(RedisModule.Json)) {
            json = new RedisJson(pool);
        }
        if (redisModule.contains(RedisModule.Graph)) {
            graph = new RedisGraphs(pool);
        }
        if (redisModule.contains(RedisModule.Search)) {
            search = new RedisSearch("test", pool);
        }
    }

    public ModulePool(Set<String> urls, RedisMode redisMode, Set<RedisModule> redisModule, int maxTotal, int maxIdle, int timeout, String password) {
        this.redisMode = redisMode;
        this.redisModule = redisModule;
        if (redisMode != RedisMode.CLUSTER)
            throw new IllegalThreadStateException("redis mode is not cluster");
        if (redisModule.contains(RedisModule.Json)) {
            Set<HostAndPort> set = urls.stream().map(it -> {
                String[] hps = it.split(":");
                return new HostAndPort(hps[0], Integer.parseInt(hps[1]));
            }).collect(Collectors.toSet());
            JedisPoolConfig config = new JedisPoolConfig();
            config.setMaxTotal(maxTotal);
            config.setMaxIdle(maxIdle);
            jsonCluster = new RedisJsonCluster(set, timeout, timeout, 10, password, config);
        }
    }

    /**
     * set redisMode -> RedisMode.STANDALONE or RedisMode.SENTINEL
     * @param r reJSON接口
     * @param <T> 返回值类型
     * @return T
     */
    public <T> T json(Function<Json, T> r) {
        if (redisMode == RedisMode.STANDALONE || redisMode == RedisMode.SENTINEL) {
            return r.apply(json);
        } else if (redisMode == RedisMode.CLUSTER) {
            return r.apply(jsonCluster);
        } else {
            throw new RuntimeException("redis mode is error!");
        }
    }

    public <T> T graph(Function<RedisGraphs, T> r) {
        return r.apply(graph);
    }

    public <T> T search(Function<RedisSearch, T> r) {
        return r.apply(search);
    }

}
