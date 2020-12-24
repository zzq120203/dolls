package com.zzq.dolls.redis.mini;

import redis.clients.jedis.Jedis;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocketFactory;

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

}
