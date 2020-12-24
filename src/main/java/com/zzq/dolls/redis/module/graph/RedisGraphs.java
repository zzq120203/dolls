package com.zzq.dolls.redis.module.graph;

import com.redislabs.redisgraph.impl.api.RedisGraph;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.util.Pool;

public class RedisGraphs extends RedisGraph implements Graph {

    public RedisGraphs() {
    }

    public RedisGraphs(String host, int port) {
        super(host, port);
    }

    public RedisGraphs(Pool<Jedis> jedis) {
        super(jedis);
    }

}
