package com.zzq.dolls.redis.mini;

import redis.clients.jedis.Jedis;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocketFactory;
import java.util.List;

public class JedisMini extends Jedis implements Redis {
    protected JedisPoolAbstractMini dataSource = null;

    public JedisMini(String host, int port, int connectionTimeout, int soTimeout, boolean ssl, SSLSocketFactory sslSocketFactory, SSLParameters sslParameters, HostnameVerifier hostnameVerifier) {
        super(host, port, connectionTimeout, soTimeout, ssl, sslSocketFactory, sslParameters, hostnameVerifier);

    }

    public JedisMini(String host, int port, int sentinelConnectionTimeout, int sentinelSoTimeout) {
        super(host, port, sentinelConnectionTimeout, sentinelSoTimeout);
    }

    public void setDataSource(JedisPoolAbstractMini jedisPool) {
        this.dataSource = jedisPool;
    }

    @Override
    public void close() {
        if (dataSource != null) {
            JedisPoolAbstractMini pool = this.dataSource;
            this.dataSource = null;
            if (client.isBroken()) {
                pool.returnBrokenResource(this);
            } else {
                pool.returnResource(this);
            }
        } else {
            super.close();
        }
    }

    @Deprecated
    @Override
    public Object eval(String script, String sampleKey) {
        return super.eval(script);
    }

    @Deprecated
    @Override
    public Object evalsha(String sha1, String sampleKey) {
        return super.evalsha(sha1);
    }

    @Deprecated
    @Override
    public Boolean scriptExists(String sha1, String sampleKey) {
        return super.scriptExists(sha1);
    }

    @Deprecated
    @Override
    public List<Boolean> scriptExists(String sampleKey, String... sha1) {
        return super.scriptExists(sha1);
    }

    @Deprecated
    @Override
    public String scriptLoad(String script, String sampleKey) {
        return super.scriptLoad(script);
    }

    @Deprecated
    @Override
    public String scriptFlush(String sampleKey) {
        return super.scriptFlush();
    }

    @Deprecated
    @Override
    public String scriptKill(String sampleKey) {
        return super.scriptKill();
    }
}
