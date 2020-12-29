package com.zzq.dolls.redis.mini;

import com.zzq.dolls.redis.RedisMode;
import com.zzq.dolls.redis.RedisPool;

import java.util.function.Function;

public class MiniPool {

    private JedisMini jedis;

    protected RedisMode mode;
    protected RedisPool pool;

    public MiniPool(RedisPool pool, RedisMode mode) {
        this.mode = mode;
        this.pool = pool;
        jedis = new JedisMini(pool, mode);
    }

    /**
     * set redisMode -> RedisMode.STANDALONE or RedisMode.SENTINEL
     * @param r redis接口
     * @param <T> 返回值类型
     * @return T
     */
    public <T> T jedis(Function<Redis, T> r) {
        return r.apply(jedis);
    }

}
