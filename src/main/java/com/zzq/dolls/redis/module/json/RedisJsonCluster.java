package com.zzq.dolls.redis.module.json;

import com.redislabs.modules.rejson.JReJSON;
import com.redislabs.modules.rejson.Path;
import com.zzq.dolls.redis.module.ModuleCluster;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.*;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocketFactory;
import java.lang.reflect.Type;
import java.util.Set;

public class RedisJsonCluster extends ModuleCluster {
    public RedisJsonCluster(HostAndPort node) {
        super(node);
    }

    public RedisJsonCluster(HostAndPort node, int timeout) {
        super(node, timeout);
    }

    public RedisJsonCluster(HostAndPort node, int timeout, int maxAttempts) {
        super(node, timeout, maxAttempts);
    }

    public RedisJsonCluster(HostAndPort node, GenericObjectPoolConfig poolConfig) {
        super(node, poolConfig);
    }

    public RedisJsonCluster(HostAndPort node, int timeout, GenericObjectPoolConfig poolConfig) {
        super(node, timeout, poolConfig);
    }

    public RedisJsonCluster(HostAndPort node, int timeout, int maxAttempts, GenericObjectPoolConfig poolConfig) {
        super(node, timeout, maxAttempts, poolConfig);
    }

    public RedisJsonCluster(HostAndPort node, int connectionTimeout, int soTimeout, int maxAttempts, GenericObjectPoolConfig poolConfig) {
        super(node, connectionTimeout, soTimeout, maxAttempts, poolConfig);
    }

    public RedisJsonCluster(HostAndPort node, int connectionTimeout, int soTimeout, int maxAttempts, String password, GenericObjectPoolConfig poolConfig) {
        super(node, connectionTimeout, soTimeout, maxAttempts, password, poolConfig);
    }

    public RedisJsonCluster(HostAndPort node, int connectionTimeout, int soTimeout, int maxAttempts, String password, String clientName, GenericObjectPoolConfig poolConfig) {
        super(node, connectionTimeout, soTimeout, maxAttempts, password, clientName, poolConfig);
    }

    public RedisJsonCluster(HostAndPort node, int connectionTimeout, int soTimeout, int maxAttempts, String user, String password, String clientName, GenericObjectPoolConfig poolConfig) {
        super(node, connectionTimeout, soTimeout, maxAttempts, user, password, clientName, poolConfig);
    }

    public RedisJsonCluster(HostAndPort node, int connectionTimeout, int soTimeout, int maxAttempts, String password, String clientName, GenericObjectPoolConfig poolConfig, boolean ssl) {
        super(node, connectionTimeout, soTimeout, maxAttempts, password, clientName, poolConfig, ssl);
    }

    public RedisJsonCluster(HostAndPort node, int connectionTimeout, int soTimeout, int maxAttempts, String user, String password, String clientName, GenericObjectPoolConfig poolConfig, boolean ssl) {
        super(node, connectionTimeout, soTimeout, maxAttempts, user, password, clientName, poolConfig, ssl);
    }

    public RedisJsonCluster(HostAndPort node, int connectionTimeout, int soTimeout, int maxAttempts, String password, String clientName, GenericObjectPoolConfig poolConfig, boolean ssl, SSLSocketFactory sslSocketFactory, SSLParameters sslParameters, HostnameVerifier hostnameVerifier, JedisClusterHostAndPortMap hostAndPortMap) {
        super(node, connectionTimeout, soTimeout, maxAttempts, password, clientName, poolConfig, ssl, sslSocketFactory, sslParameters, hostnameVerifier, hostAndPortMap);
    }

    public RedisJsonCluster(HostAndPort node, int connectionTimeout, int soTimeout, int maxAttempts, String user, String password, String clientName, GenericObjectPoolConfig poolConfig, boolean ssl, SSLSocketFactory sslSocketFactory, SSLParameters sslParameters, HostnameVerifier hostnameVerifier, JedisClusterHostAndPortMap hostAndPortMap) {
        super(node, connectionTimeout, soTimeout, maxAttempts, user, password, clientName, poolConfig, ssl, sslSocketFactory, sslParameters, hostnameVerifier, hostAndPortMap);
    }

    public RedisJsonCluster(Set<HostAndPort> nodes) {
        super(nodes);
    }

    public RedisJsonCluster(Set<HostAndPort> nodes, int timeout) {
        super(nodes, timeout);
    }

    public RedisJsonCluster(Set<HostAndPort> nodes, int timeout, int maxAttempts) {
        super(nodes, timeout, maxAttempts);
    }

    public RedisJsonCluster(Set<HostAndPort> nodes, GenericObjectPoolConfig poolConfig) {
        super(nodes, poolConfig);
    }

    public RedisJsonCluster(Set<HostAndPort> nodes, int timeout, GenericObjectPoolConfig poolConfig) {
        super(nodes, timeout, poolConfig);
    }

    public RedisJsonCluster(Set<HostAndPort> jedisClusterNode, int timeout, int maxAttempts, GenericObjectPoolConfig poolConfig) {
        super(jedisClusterNode, timeout, maxAttempts, poolConfig);
    }

    public RedisJsonCluster(Set<HostAndPort> jedisClusterNode, int connectionTimeout, int soTimeout, int maxAttempts, GenericObjectPoolConfig poolConfig) {
        super(jedisClusterNode, connectionTimeout, soTimeout, maxAttempts, poolConfig);
    }

    public RedisJsonCluster(Set<HostAndPort> jedisClusterNode, int connectionTimeout, int soTimeout, int maxAttempts, String password, GenericObjectPoolConfig poolConfig) {
        super(jedisClusterNode, connectionTimeout, soTimeout, maxAttempts, password, poolConfig);
    }

    public RedisJsonCluster(Set<HostAndPort> jedisClusterNode, int connectionTimeout, int soTimeout, int maxAttempts, String password, String clientName, GenericObjectPoolConfig poolConfig) {
        super(jedisClusterNode, connectionTimeout, soTimeout, maxAttempts, password, clientName, poolConfig);
    }

    public RedisJsonCluster(Set<HostAndPort> jedisClusterNode, int connectionTimeout, int soTimeout, int maxAttempts, String user, String password, String clientName, GenericObjectPoolConfig poolConfig) {
        super(jedisClusterNode, connectionTimeout, soTimeout, maxAttempts, user, password, clientName, poolConfig);
    }

    public RedisJsonCluster(Set<HostAndPort> jedisClusterNode, int connectionTimeout, int soTimeout, int maxAttempts, String password, String clientName, GenericObjectPoolConfig poolConfig, boolean ssl) {
        super(jedisClusterNode, connectionTimeout, soTimeout, maxAttempts, password, clientName, poolConfig, ssl);
    }

    public RedisJsonCluster(Set<HostAndPort> jedisClusterNode, int connectionTimeout, int soTimeout, int maxAttempts, String user, String password, String clientName, GenericObjectPoolConfig poolConfig, boolean ssl) {
        super(jedisClusterNode, connectionTimeout, soTimeout, maxAttempts, user, password, clientName, poolConfig, ssl);
    }

    public RedisJsonCluster(Set<HostAndPort> jedisClusterNode, int connectionTimeout, int soTimeout, int maxAttempts, String password, String clientName, GenericObjectPoolConfig poolConfig, boolean ssl, SSLSocketFactory sslSocketFactory, SSLParameters sslParameters, HostnameVerifier hostnameVerifier, JedisClusterHostAndPortMap hostAndPortMap) {
        super(jedisClusterNode, connectionTimeout, soTimeout, maxAttempts, password, clientName, poolConfig, ssl, sslSocketFactory, sslParameters, hostnameVerifier, hostAndPortMap);
    }

    public RedisJsonCluster(Set<HostAndPort> jedisClusterNode, int connectionTimeout, int soTimeout, int maxAttempts, String user, String password, String clientName, GenericObjectPoolConfig poolConfig, boolean ssl, SSLSocketFactory sslSocketFactory, SSLParameters sslParameters, HostnameVerifier hostnameVerifier, JedisClusterHostAndPortMap hostAndPortMap) {
        super(jedisClusterNode, connectionTimeout, soTimeout, maxAttempts, user, password, clientName, poolConfig, ssl, sslSocketFactory, sslParameters, hostnameVerifier, hostAndPortMap);
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
        return null;
    }

    @Override
    public <T> T get(String key, Path... paths) {
        return null;
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
    public void set(String key, Object object, JReJSON.ExistenceModifier flag) {
        set(key, object, flag, Path.ROOT_PATH);
    }

    /**
     * Sets an object in the root path
     * @param key the key name
     * @param object the Java object to store
     */
    @Override
    public void set(String key, Object object) {
        set(key, object, JReJSON.ExistenceModifier.DEFAULT, Path.ROOT_PATH);
    }

    /**
     * Sets an object without caring about target path existing
     * @param key the key name
     * @param object the Java object to store
     * @param path in the object
     */
    @Override
    public void set(String key, Object object, Path path) {
        set(key, object, JReJSON.ExistenceModifier.DEFAULT, path);
    }

    @Override
    public void set(String key, Object object, JReJSON.ExistenceModifier flag, Path path) {
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
                return RedisJson.setnx(connection, key, object, path);
            }
        }.run(key);
    }

    @Override
    public void strAppend(String key, Object object, Path path) {
        new JedisClusterCommand<Boolean>(connectionHandler, maxAttempts) {
            @Override
            public Boolean execute(Jedis connection) {
                return RedisJson.setnx(connection, key, object, path);
            }
        }.run(key);
    }

    @Override
    public Class<?> type(String key, Path path) {
        return null;
    }


}
