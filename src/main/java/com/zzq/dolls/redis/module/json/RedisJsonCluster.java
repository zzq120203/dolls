package com.zzq.dolls.redis.module.json;

import com.zzq.dolls.redis.module.ModuleCluster;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.*;

import java.lang.reflect.Type;
import java.util.Set;

public class RedisJsonCluster extends ModuleCluster {

    public RedisJsonCluster(Set<HostAndPort> jedisClusterNode, int connectionTimeout, int soTimeout, int maxAttempts, String password, GenericObjectPoolConfig poolConfig) {
        super(jedisClusterNode, connectionTimeout, soTimeout, maxAttempts, password, poolConfig);
    }


    @Override
    public Long del(final String key, final Path path) {
        return new JedisClusterCommand<Long>(connectionHandler, maxAttempts) {
            @Override
            public Long execute(Jedis connection) {
                return RedisJson.del(connection,key, path);
            }
        }.run(key);
    }

    @Override
    public <T> T get(String key, Type type, Path... paths) {
        return new JedisClusterCommand<T>(connectionHandler, maxAttempts) {
            @Override
            public T execute(Jedis connection) {
                return RedisJson.get(connection, key, type, paths);
            }
        }.run(key);
    }

    @Override
    public <T> T get(String key, Path... paths) {
        return new JedisClusterCommand<T>(connectionHandler, maxAttempts) {
            @Override
            public T execute(Jedis connection) {
                return (T) RedisJson.get(connection, key, Object.class, paths);
            }
        }.run(key);
    }

    /**
     * Gets an object
     * @param key the key name
     * @param clazz
     * @param paths optional one ore more paths in the object
     * @return the requested object
     */
    @Override
    public <T> T get(String key, Class<T> clazz, Path... paths) {
        return new JedisClusterCommand<T>(connectionHandler, maxAttempts) {
            @Override
            public T execute(Jedis connection) {
                return RedisJson.get(connection, key, clazz, paths);
            }
        }.run(key);
    }

    /**
     * Sets an object at the root path
     * @param key the key name
     * @param object the Java object to store
     * @param flag an existential modifier
     */
    @Override
    public void set(String key, Object object, ExistenceModifier flag) {
        set(key, object, flag, Path.ROOT_PATH);
    }

    /**
     * Sets an object in the root path
     * @param key the key name
     * @param object the Java object to store
     */
    @Override
    public void set(String key, Object object) {
        set(key, object, ExistenceModifier.DEFAULT, Path.ROOT_PATH);
    }

    /**
     * Sets an object without caring about target path existing
     * @param key the key name
     * @param object the Java object to store
     * @param path in the object
     */
    @Override
    public void set(String key, Object object, Path path) {
        set(key, object, ExistenceModifier.DEFAULT, path);
    }

    @Override
    public void set(String key, Object object, ExistenceModifier flag, Path path) {
        new JedisClusterCommand<Integer>(connectionHandler, maxAttempts) {
            @Override
            public Integer execute(Jedis connection) {
                RedisJson.set(connection, key, object, flag, path);
                return 1;
            }
        }.run(key);
    }

    @Override
    public Boolean setnx(String key, Object object, Path path) {
        return new JedisClusterCommand<Boolean>(connectionHandler, maxAttempts) {
            @Override
            public Boolean execute(Jedis connection) {
                return RedisJson.setnx(connection, key, object, path);
            }
        }.run(key);
    }

    @Override
    public void arrAppend(String key, Object object, Path path) {
        new JedisClusterCommand<Boolean>(connectionHandler, maxAttempts) {
            @Override
            public Boolean execute(Jedis connection) {
                RedisJson.arrAppend(connection, key, object, path);
                return true;
            }
        }.run(key);
    }

    @Override
    public void strAppend(String key, Object object, Path path) {
        new JedisClusterCommand<Boolean>(connectionHandler, maxAttempts) {
            @Override
            public Boolean execute(Jedis connection) {
                RedisJson.strAppend(connection, key, object, path);
                return true;
            }
        }.run(key);
    }

    @Override
    public Class<?> type(String key, Path path) {
        return null;
    }


}
