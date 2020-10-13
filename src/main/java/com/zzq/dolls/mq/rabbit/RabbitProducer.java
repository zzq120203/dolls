package com.zzq.dolls.mq.rabbit;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class RabbitProducer {

    private String host;
    private int port;
    private String user;
    private String password;
    private String topic;

    private boolean automaticRecoveryEnabled = true;
    private long networkRecoveryInterval = 5000L;

    private int basicQos = 0;

    private Connection connection;
    private Channel channel;

    public RabbitProducer(String host, int port, String user, String password, String topic,
            boolean automaticRecoveryEnabled, long networkRecoveryInterval, int basicQos)
            throws IOException, TimeoutException {
        this.host = host;
        this.port = port;
        this.user = user;
        this.password = password;
        this.topic = topic;
        this.automaticRecoveryEnabled = automaticRecoveryEnabled;
        this.networkRecoveryInterval = networkRecoveryInterval;
        this.basicQos = basicQos;

        init();
    }

    public RabbitProducer(Builder builder) throws IOException, TimeoutException {
        this(builder.host, builder.port, builder.user, builder.password, builder.topic,
                builder.automaticRecoveryEnabled, builder.networkRecoveryInterval, builder.basicQos);
    }

    private void init() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();

        factory.setHost(host);
        factory.setPort(port);
        factory.setUsername(user);
        factory.setPassword(password);

        factory.setAutomaticRecoveryEnabled(automaticRecoveryEnabled);
        factory.setNetworkRecoveryInterval(networkRecoveryInterval);

        connection = factory.newConnection();
        channel = connection.createChannel();
        channel.basicQos(basicQos);
        // 持久化
        boolean durable = true;
        // 独占
        boolean exclusive = false;
        // 是否自动删除
        boolean autoDelete = false;
        channel.queueDeclare(topic, durable, exclusive, autoDelete, null);

    }

    public void send(String data) throws UnsupportedEncodingException, IOException {
        send((data).getBytes("UTF-8"));
    }

    public void send(byte[] data) throws UnsupportedEncodingException, IOException {
        channel.basicPublish("", topic, null, data);
    }

    public RabbitProducer start() {
        return this;
    }

    public RabbitProducer stop() throws IOException {
        try {
            channel.close();
            connection.close();
        } catch (Exception e) {
            throw new IOException(e.getMessage());
        }
        return this;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String host;
        private int port;
        private String user;
        private String password;
        private String topic;

        private boolean automaticRecoveryEnabled = true;
        private long networkRecoveryInterval = 5000L;

        private int basicQos = 0;

        /**
         *
         * 通过Builder RabbitProducer
         * 
         * @return RabbitProducer
         * @throws Exception
         */
        public RabbitProducer build() throws Exception {
            return new RabbitProducer(this);
        }

        /**
         *
         * @param url RabbitConsumer 地址
         * @return Builder
         */
        public Builder url(String url) {
            if (url == null || url.isEmpty()) {
                throw new RuntimeException("url must not be null!");
            }
            this.host = url.split(":")[0];
            this.port = Integer.parseInt(url.split(":")[1]);
            return this;
        }

        /**
         *
         * @param host RabbitConsumer IP
         * @return Builder
         */
        public Builder host(String host) {
            this.host = host;
            return this;
        }

        /**
         *
         * @param port RabbitConsumer PORT
         * @return Builder
         */
        public Builder port(int port) {
            this.port = port;
            return this;
        }

        /**
         *
         * @param user RabbitConsumer user
         * @return Builder
         */
        public Builder user(String user) {
            this.user = user;
            return this;
        }

        /**
         *
         * @param password RabbitConsumer password
         * @return Builder
         */
        public Builder password(String password) {
            this.password = password;
            return this;
        }

        /**
         *
         * @param topic RabbitConsumer topic
         * @return Builder
         */
        public Builder topic(String topic) {
            this.topic = topic;
            return this;
        }

        /**
         *
         * @param automaticRecoveryEnabled RabbitConsumer automaticRecoveryEnabled
         * @return Builder
         */
        public Builder automaticRecoveryEnabled(boolean automaticRecoveryEnabled) {
            this.automaticRecoveryEnabled = automaticRecoveryEnabled;
            return this;
        }

        /**
         *
         * @param networkRecoveryInterval RabbitConsumer networkRecoveryInterval
         * @return Builder
         */
        public Builder networkRecoveryInterval(long networkRecoveryInterval) {
            this.networkRecoveryInterval = networkRecoveryInterval;
            return this;
        }

        /**
         *
         * @param basicQos RabbitConsumer basicQos
         * @return Builder
         */
        public Builder basicQos(int basicQos) {
            this.basicQos = basicQos;
            return this;
        }

    }
}
