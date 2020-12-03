package com.zzq.dolls.redis.module;

import com.redislabs.modules.rejson.JReJSON;
import com.redislabs.redisgraph.impl.api.RedisGraph;
import com.zzq.dolls.redis.RedisPool;
import com.zzq.dolls.redis.RedisMode;
import io.redisearch.client.Client;

import java.util.Set;
import java.util.function.Function;

public class ModulePool extends RedisPool {

    private RedisJson json;

    private RedisGraph graph;

    private Client search;

    public ModulePool(Builder builder) {
        super(builder);
    }

    public ModulePool(Set<String> urls, String password, RedisMode redisMode, int timeout, int maxTotal, int maxIdle, String masterName, int db, Set<RedisModule> redisModule) {
        super(urls, password, redisMode, timeout, maxTotal, maxIdle, masterName, db, redisModule);
    }

    @Override
    protected void init() {
        if (redisMode != RedisMode.STANDALONE && redisMode != RedisMode.SENTINEL)
            throw new IllegalThreadStateException("redis mode is not standalone or sentinel");
        super.init();
        if (this.redisModule.contains(RedisModule.Json)) {
            json = new RedisJson(super.pool);
        }
        if (this.redisModule.contains(RedisModule.Graph)) {
            graph = new RedisGraph(super.pool);
        }
        if (this.redisModule.contains(RedisModule.Search)) {
            search = new Client("test", super.pool);
        }
    }

    /**
     * set redisMode -> RedisMode.STANDALONE or RedisMode.SENTINEL
     * @param r reJSON接口
     * @param <T> 返回值类型
     * @return T
     */
    @Override
    public <T> T json(Function<RedisJson, T> r) {
        return r.apply(json);
    }

    @Override
    public <T> T graph(Function<RedisGraph, T> r) {
        return r.apply(graph);
    }

    @Override
    public <T> T search(Function<Client, T> r) {
        return r.apply(search);
    }

}
