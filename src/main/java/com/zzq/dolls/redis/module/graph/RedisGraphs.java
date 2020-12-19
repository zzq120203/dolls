package com.zzq.dolls.redis.module.graph;

import com.redislabs.redisgraph.impl.api.RedisGraph;
import com.zzq.dolls.redis.module.ModulePipeline;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.util.Pool;

public class RedisGraphs extends RedisGraph {

    public RedisGraphs() {
    }

    public RedisGraphs(String host, int port) {
        super(host, port);
    }

    public RedisGraphs(Pool<Jedis> jedis) {
        super(jedis);
    }

    public ModulePipeline pipelined() {
        ModulePipeline pipeline = new ModulePipeline();
        pipeline.setClient(getConnection().getClient());
        return pipeline;
    }
}
