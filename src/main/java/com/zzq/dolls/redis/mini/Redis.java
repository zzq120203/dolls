package com.zzq.dolls.redis.mini;

import redis.clients.jedis.commands.BinaryJedisClusterCommands;
import redis.clients.jedis.commands.JedisClusterBinaryScriptingCommands;
import redis.clients.jedis.commands.MultiKeyBinaryJedisClusterCommands;

import java.io.Closeable;
import java.util.List;

public interface Redis extends BinaryJedisClusterCommands,
        MultiKeyBinaryJedisClusterCommands, JedisClusterBinaryScriptingCommands, Closeable {
    @Override
    default Long waitReplicas(byte[] key, int replicas, long timeout) {
        return null;
    }
    @Override
    default Object eval(byte[] script, byte[] sampleKey) {
        return null;
    }
    @Override
    default Object evalsha(byte[] sha1, byte[] sampleKey) {
        return null;
    }

    @Override
    default List<Long> scriptExists(byte[] sampleKey, byte[]... sha1) {
        return null;
    }

    @Override
    default byte[] scriptLoad(byte[] script, byte[] sampleKey) {
        return new byte[0];
    }

    @Override
    default String scriptFlush(byte[] sampleKey) {
        return null;
    }

    @Override
    default String scriptKill(byte[] sampleKey) {
        return null;
    }
}
