package com.zzq.dolls.redis.mini;


import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.exceptions.JedisException;


class JedisPoolMini extends JedisPoolAbstractMini {
    public JedisPoolMini(GenericObjectPoolConfig poolConfig, final String host, int port,
                     int timeout, final String password, final int database) {
        super(poolConfig, host, port, timeout, password, database);
    }

    @Override
    public JedisMini getResource() {
        JedisMini jedis = super.getResource();
        jedis.setDataSource(this);
        return jedis;
    }

    @Override
    protected void returnBrokenResource(final JedisMini resource) {
        if (resource != null) {
            returnBrokenResourceObject(resource);
        }
    }

    @Override
    protected void returnResource(final JedisMini resource) {
        if (resource != null) {
            try {
                resource.resetState();
                returnResourceObject(resource);
            } catch (Exception e) {
                returnBrokenResource(resource);
                throw new JedisException("Resource is returned to the pool as broken", e);
            }
        }
    }


}

