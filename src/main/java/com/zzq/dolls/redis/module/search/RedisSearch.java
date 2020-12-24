package com.zzq.dolls.redis.module.search;

import io.redisearch.client.Client;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.util.Pool;


public class RedisSearch extends Client implements Search{
    public RedisSearch(String indexName, Pool<Jedis> pool) {
        super(indexName, pool);
    }
}
