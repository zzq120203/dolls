package zzq.dolls.redis;

import redis.clients.jedis.Jedis;

public interface Handler<T> {
    T apply(Jedis jedis);
}