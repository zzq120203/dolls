package com.zzq.dolls.redis.module;

import com.zzq.dolls.redis.module.graph.Graph;
import com.zzq.dolls.redis.module.json.Json;
import com.zzq.dolls.redis.module.search.Search;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.util.Set;

public abstract class ModuleCluster extends JedisCluster implements Json, Search, Graph {

    public ModuleCluster(Set<HostAndPort> jedisClusterNode, int connectionTimeout, int soTimeout, int maxAttempts, String password, GenericObjectPoolConfig poolConfig) {
        super(jedisClusterNode, connectionTimeout, soTimeout, maxAttempts, password, poolConfig);
    }

}
