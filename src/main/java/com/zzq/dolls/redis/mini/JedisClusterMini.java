package com.zzq.dolls.redis.mini;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.*;

import java.util.List;
import java.util.Set;

public class JedisClusterMini extends JedisCluster implements Redis {
    private final String sampleKey = "def";

    public JedisClusterMini(Set<HostAndPort> jedisClusterNode, int connectionTimeout, int soTimeout, int maxAttempts, String password, GenericObjectPoolConfig poolConfig) {
        super(jedisClusterNode, connectionTimeout, soTimeout, maxAttempts, password, poolConfig);
    }

    @Deprecated
    @Override
    public Object eval(String script) {
        return super.eval(script, sampleKey);
    }

    @Deprecated
    @Override
    public Object evalsha(String sha1) {
        return super.eval(sha1, sampleKey);
    }

    @Deprecated
    @Override
    public Boolean scriptExists(String sha1) {
        return super.scriptExists(sha1, sampleKey);
    }

    @Deprecated
    @Override
    public List<Boolean> scriptExists(String... sha1) {
        return super.scriptExists(sampleKey, sha1);
    }

    @Deprecated
    @Override
    public String scriptLoad(String script) {
        return super.scriptLoad(script, sampleKey);
    }
}
