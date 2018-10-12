package zzq.dolls.redis;

import redis.clients.jedis.JedisCluster;

public interface Cluster<T> {
    T apply(JedisCluster cluster);
}