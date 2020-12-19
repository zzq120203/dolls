package com.zzq.dolls.redis.module;

import com.zzq.dolls.redis.module.graph.Graph;
import com.zzq.dolls.redis.module.json.Json;
import com.zzq.dolls.redis.module.search.Search;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisClusterHostAndPortMap;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocketFactory;
import java.util.Set;

public abstract class ModuleCluster extends JedisCluster implements Json, Search, Graph {
    public ModuleCluster(HostAndPort node) {
        super(node);
    }

    public ModuleCluster(HostAndPort node, int timeout) {
        super(node, timeout);
    }

    public ModuleCluster(HostAndPort node, int timeout, int maxAttempts) {
        super(node, timeout, maxAttempts);
    }

    public ModuleCluster(HostAndPort node, GenericObjectPoolConfig poolConfig) {
        super(node, poolConfig);
    }

    public ModuleCluster(HostAndPort node, int timeout, GenericObjectPoolConfig poolConfig) {
        super(node, timeout, poolConfig);
    }

    public ModuleCluster(HostAndPort node, int timeout, int maxAttempts, GenericObjectPoolConfig poolConfig) {
        super(node, timeout, maxAttempts, poolConfig);
    }

    public ModuleCluster(HostAndPort node, int connectionTimeout, int soTimeout, int maxAttempts, GenericObjectPoolConfig poolConfig) {
        super(node, connectionTimeout, soTimeout, maxAttempts, poolConfig);
    }

    public ModuleCluster(HostAndPort node, int connectionTimeout, int soTimeout, int maxAttempts, String password, GenericObjectPoolConfig poolConfig) {
        super(node, connectionTimeout, soTimeout, maxAttempts, password, poolConfig);
    }

    public ModuleCluster(HostAndPort node, int connectionTimeout, int soTimeout, int maxAttempts, String password, String clientName, GenericObjectPoolConfig poolConfig) {
        super(node, connectionTimeout, soTimeout, maxAttempts, password, clientName, poolConfig);
    }

    public ModuleCluster(HostAndPort node, int connectionTimeout, int soTimeout, int maxAttempts, String user, String password, String clientName, GenericObjectPoolConfig poolConfig) {
        super(node, connectionTimeout, soTimeout, maxAttempts, user, password, clientName, poolConfig);
    }

    public ModuleCluster(HostAndPort node, int connectionTimeout, int soTimeout, int maxAttempts, String password, String clientName, GenericObjectPoolConfig poolConfig, boolean ssl) {
        super(node, connectionTimeout, soTimeout, maxAttempts, password, clientName, poolConfig, ssl);
    }

    public ModuleCluster(HostAndPort node, int connectionTimeout, int soTimeout, int maxAttempts, String user, String password, String clientName, GenericObjectPoolConfig poolConfig, boolean ssl) {
        super(node, connectionTimeout, soTimeout, maxAttempts, user, password, clientName, poolConfig, ssl);
    }

    public ModuleCluster(HostAndPort node, int connectionTimeout, int soTimeout, int maxAttempts, String password, String clientName, GenericObjectPoolConfig poolConfig, boolean ssl, SSLSocketFactory sslSocketFactory, SSLParameters sslParameters, HostnameVerifier hostnameVerifier, JedisClusterHostAndPortMap hostAndPortMap) {
        super(node, connectionTimeout, soTimeout, maxAttempts, password, clientName, poolConfig, ssl, sslSocketFactory, sslParameters, hostnameVerifier, hostAndPortMap);
    }

    public ModuleCluster(HostAndPort node, int connectionTimeout, int soTimeout, int maxAttempts, String user, String password, String clientName, GenericObjectPoolConfig poolConfig, boolean ssl, SSLSocketFactory sslSocketFactory, SSLParameters sslParameters, HostnameVerifier hostnameVerifier, JedisClusterHostAndPortMap hostAndPortMap) {
        super(node, connectionTimeout, soTimeout, maxAttempts, user, password, clientName, poolConfig, ssl, sslSocketFactory, sslParameters, hostnameVerifier, hostAndPortMap);
    }

    public ModuleCluster(Set<HostAndPort> nodes) {
        super(nodes);
    }

    public ModuleCluster(Set<HostAndPort> nodes, int timeout) {
        super(nodes, timeout);
    }

    public ModuleCluster(Set<HostAndPort> nodes, int timeout, int maxAttempts) {
        super(nodes, timeout, maxAttempts);
    }

    public ModuleCluster(Set<HostAndPort> nodes, GenericObjectPoolConfig poolConfig) {
        super(nodes, poolConfig);
    }

    public ModuleCluster(Set<HostAndPort> nodes, int timeout, GenericObjectPoolConfig poolConfig) {
        super(nodes, timeout, poolConfig);
    }

    public ModuleCluster(Set<HostAndPort> jedisClusterNode, int timeout, int maxAttempts, GenericObjectPoolConfig poolConfig) {
        super(jedisClusterNode, timeout, maxAttempts, poolConfig);
    }

    public ModuleCluster(Set<HostAndPort> jedisClusterNode, int connectionTimeout, int soTimeout, int maxAttempts, GenericObjectPoolConfig poolConfig) {
        super(jedisClusterNode, connectionTimeout, soTimeout, maxAttempts, poolConfig);
    }

    public ModuleCluster(Set<HostAndPort> jedisClusterNode, int connectionTimeout, int soTimeout, int maxAttempts, String password, GenericObjectPoolConfig poolConfig) {
        super(jedisClusterNode, connectionTimeout, soTimeout, maxAttempts, password, poolConfig);
    }

    public ModuleCluster(Set<HostAndPort> jedisClusterNode, int connectionTimeout, int soTimeout, int maxAttempts, String password, String clientName, GenericObjectPoolConfig poolConfig) {
        super(jedisClusterNode, connectionTimeout, soTimeout, maxAttempts, password, clientName, poolConfig);
    }

    public ModuleCluster(Set<HostAndPort> jedisClusterNode, int connectionTimeout, int soTimeout, int maxAttempts, String user, String password, String clientName, GenericObjectPoolConfig poolConfig) {
        super(jedisClusterNode, connectionTimeout, soTimeout, maxAttempts, user, password, clientName, poolConfig);
    }

    public ModuleCluster(Set<HostAndPort> jedisClusterNode, int connectionTimeout, int soTimeout, int maxAttempts, String password, String clientName, GenericObjectPoolConfig poolConfig, boolean ssl) {
        super(jedisClusterNode, connectionTimeout, soTimeout, maxAttempts, password, clientName, poolConfig, ssl);
    }

    public ModuleCluster(Set<HostAndPort> jedisClusterNode, int connectionTimeout, int soTimeout, int maxAttempts, String user, String password, String clientName, GenericObjectPoolConfig poolConfig, boolean ssl) {
        super(jedisClusterNode, connectionTimeout, soTimeout, maxAttempts, user, password, clientName, poolConfig, ssl);
    }

    public ModuleCluster(Set<HostAndPort> jedisClusterNode, int connectionTimeout, int soTimeout, int maxAttempts, String password, String clientName, GenericObjectPoolConfig poolConfig, boolean ssl, SSLSocketFactory sslSocketFactory, SSLParameters sslParameters, HostnameVerifier hostnameVerifier, JedisClusterHostAndPortMap hostAndPortMap) {
        super(jedisClusterNode, connectionTimeout, soTimeout, maxAttempts, password, clientName, poolConfig, ssl, sslSocketFactory, sslParameters, hostnameVerifier, hostAndPortMap);
    }

    public ModuleCluster(Set<HostAndPort> jedisClusterNode, int connectionTimeout, int soTimeout, int maxAttempts, String user, String password, String clientName, GenericObjectPoolConfig poolConfig, boolean ssl, SSLSocketFactory sslSocketFactory, SSLParameters sslParameters, HostnameVerifier hostnameVerifier, JedisClusterHostAndPortMap hostAndPortMap) {
        super(jedisClusterNode, connectionTimeout, soTimeout, maxAttempts, user, password, clientName, poolConfig, ssl, sslSocketFactory, sslParameters, hostnameVerifier, hostAndPortMap);
    }

}
