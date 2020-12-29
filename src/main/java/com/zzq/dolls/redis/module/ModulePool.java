package com.zzq.dolls.redis.module;

import com.zzq.dolls.redis.RedisMode;
import com.zzq.dolls.redis.RedisPool;
import com.zzq.dolls.redis.mini.MiniPool;
import com.zzq.dolls.redis.module.json.Json;
import com.zzq.dolls.redis.module.json.RedisJson;

import java.util.Set;
import java.util.function.Function;

public class ModulePool extends MiniPool {

    private RedisJson json;

    private Set<RedisModule> modules;

    public ModulePool(RedisPool pool, RedisMode mode, Set<RedisModule> modules) {
        super(pool, mode);
        this.modules = modules;
        init();
    }

    private void init() {
        if (modules.contains(RedisModule.Json)) {
            json = new RedisJson(pool, mode);
        }
    }

    /**
     * set redisMode -> RedisMode.STANDALONE or RedisMode.SENTINEL
     * @param r reJSON接口
     * @param <T> 返回值类型
     * @return T
     */
    public <T> T json(Function<Json, T> r) {
        return r.apply(json);
    }

}
