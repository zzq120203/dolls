package com.zzq.dolls.redis.module;

import com.zzq.dolls.redis.RedisPool;
import com.zzq.dolls.redis.RedisMode;

import java.util.Set;
import java.util.function.Function;

public class ModulePool extends RedisPool {

    private RedisJson json;

    private RedisGraphs graph;

    private RedisSearch search;

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
            graph = new RedisGraphs(super.pool);
        }
        if (this.redisModule.contains(RedisModule.Search)) {
            search = new RedisSearch("test", super.pool);
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
    public <T> T graph(Function<RedisGraphs, T> r) {
        return r.apply(graph);
    }

    @Override
    public <T> T search(Function<RedisSearch, T> r) {
        return r.apply(search);
    }

}
