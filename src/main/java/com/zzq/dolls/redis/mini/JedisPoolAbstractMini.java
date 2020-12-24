package com.zzq.dolls.redis.mini;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.exceptions.InvalidURIException;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.jedis.util.JedisURIHelper;
import redis.clients.jedis.util.Pool;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocketFactory;
import java.net.URI;
import java.util.concurrent.atomic.AtomicReference;

public class JedisPoolAbstractMini extends Pool<JedisMini> {

    public JedisPoolAbstractMini() {
        super();
    }

    public JedisPoolAbstractMini(final GenericObjectPoolConfig poolConfig, final String host, int port,
                     final int connectionTimeout, final int soTimeout, final String password, final int database,
                     final String clientName) {
        super(poolConfig, new JedisFactoryMini(host, port, connectionTimeout, soTimeout, password,
                database, clientName));
    }

    public JedisPoolAbstractMini(GenericObjectPoolConfig poolConfig, String host, int port, int timeout, String password, int database) {
        this(poolConfig, host, port, timeout, timeout, password, database, null);
    }


    @Override
    protected void returnBrokenResource(JedisMini resource) {
        super.returnBrokenResource(resource);
    }

    @Override
    protected void returnResource(JedisMini resource) {
        super.returnResource(resource);
    }

    static class JedisFactoryMini implements PooledObjectFactory<JedisMini> {
        private final AtomicReference<HostAndPort> hostAndPort = new AtomicReference<HostAndPort>();
        private final int connectionTimeout;
        private final int soTimeout;
        private final String user;
        private final String password;
        private final int database;
        private final String clientName;
        private final boolean ssl;
        private final SSLSocketFactory sslSocketFactory;
        private final SSLParameters sslParameters;
        private final HostnameVerifier hostnameVerifier;

        JedisFactoryMini(final String host, final int port, final int connectionTimeout,
                         final int soTimeout, final String password, final int database, final String clientName) {
            this(host, port, connectionTimeout, soTimeout, password, database, clientName,
                    false, null, null, null);
        }

        JedisFactoryMini(final String host, final int port, final int connectionTimeout,
                         final int soTimeout, final String user, final String password, final int database, final String clientName) {
            this(host, port, connectionTimeout, soTimeout, user, password, database, clientName,
                    false, null, null, null);
        }

        JedisFactoryMini(final String host, final int port, final int connectionTimeout,
                         final int soTimeout, final String password, final int database, final String clientName,
                         final boolean ssl, final SSLSocketFactory sslSocketFactory, final SSLParameters sslParameters,
                         final HostnameVerifier hostnameVerifier) {
            this(host, port, connectionTimeout, soTimeout, null, password, database, clientName,
                    ssl, sslSocketFactory, sslParameters, hostnameVerifier);
        }

        JedisFactoryMini(final String host, final int port, final int connectionTimeout,
                         final int soTimeout, final String user, final String password, final int database, final String clientName,
                         final boolean ssl, final SSLSocketFactory sslSocketFactory, final SSLParameters sslParameters,
                         final HostnameVerifier hostnameVerifier) {
            this.hostAndPort.set(new HostAndPort(host, port));
            this.connectionTimeout = connectionTimeout;
            this.soTimeout = soTimeout;
            this.user = user;
            this.password = password;
            this.database = database;
            this.clientName = clientName;
            this.ssl = ssl;
            this.sslSocketFactory = sslSocketFactory;
            this.sslParameters = sslParameters;
            this.hostnameVerifier = hostnameVerifier;
        }

        JedisFactoryMini(final URI uri, final int connectionTimeout, final int soTimeout,
                         final String clientName) {
            this(uri, connectionTimeout, soTimeout, clientName, null, null, null);
        }

        JedisFactoryMini(final URI uri, final int connectionTimeout, final int soTimeout,
                         final String clientName, final SSLSocketFactory sslSocketFactory,
                         final SSLParameters sslParameters, final HostnameVerifier hostnameVerifier) {
            if (!JedisURIHelper.isValid(uri)) {
                throw new InvalidURIException(String.format(
                        "Cannot open Redis connection due invalid URI. %s", uri.toString()));
            }

            this.hostAndPort.set(new HostAndPort(uri.getHost(), uri.getPort()));
            this.connectionTimeout = connectionTimeout;
            this.soTimeout = soTimeout;
            this.user = JedisURIHelper.getUser(uri);
            this.password = JedisURIHelper.getPassword(uri);
            this.database = JedisURIHelper.getDBIndex(uri);
            this.clientName = clientName;
            this.ssl = JedisURIHelper.isRedisSSLScheme(uri);
            this.sslSocketFactory = sslSocketFactory;
            this.sslParameters = sslParameters;
            this.hostnameVerifier = hostnameVerifier;
        }

        public void setHostAndPort(final HostAndPort hostAndPort) {
            this.hostAndPort.set(hostAndPort);
        }

        @Override
        public void activateObject(PooledObject<JedisMini> pooledJedis) throws Exception {
            final JedisMini jedis = pooledJedis.getObject();
            if (jedis.getDB() != database) {
                jedis.select(database);
            }
        }

        @Override
        public void destroyObject(PooledObject<JedisMini> pooledJedis) throws Exception {
            final JedisMini jedis = pooledJedis.getObject();
            if (jedis.isConnected()) {
                try {
                    try {
                        jedis.quit();
                    } catch (Exception e) {
                    }
                    jedis.disconnect();
                } catch (Exception e) {
                }
            }
        }

        @Override
        public PooledObject<JedisMini> makeObject() throws Exception {
            final HostAndPort hp = this.hostAndPort.get();
            final JedisMini jedis = new JedisMini(hp.getHost(), hp.getPort(), connectionTimeout, soTimeout,
                    ssl, sslSocketFactory, sslParameters, hostnameVerifier);
            try {
                jedis.connect();
                if (user != null) {
                    jedis.auth(user, password);
                } else if (password != null) {
                    jedis.auth(password);
                }
                if (database != 0) {
                    jedis.select(database);
                }
                if (clientName != null) {
                    jedis.clientSetname(clientName);
                }
            } catch (JedisException je) {
                jedis.close();
                throw je;
            }

            return new DefaultPooledObject<>(jedis);
        }

        @Override
        public void passivateObject(PooledObject<JedisMini> pooledJedis) throws Exception {
            // TODO maybe should select db 0? Not sure right now.
        }

        @Override
        public boolean validateObject(PooledObject<JedisMini> pooledJedis) {
            final JedisMini jedis = pooledJedis.getObject();
            try {
                HostAndPort hostAndPort = this.hostAndPort.get();

                String connectionHost = jedis.getClient().getHost();
                int connectionPort = jedis.getClient().getPort();

                return hostAndPort.getHost().equals(connectionHost)
                        && hostAndPort.getPort() == connectionPort && jedis.isConnected()
                        && jedis.ping().equals("PONG");
            } catch (final Exception e) {
                return false;
            }
        }
    }
}
