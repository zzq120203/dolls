package zzq.dolls.mq.rocket;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

public class RocketProducer {

    private String group;
    private String nameSrv;
    private String instanceName;
    private String topic;
    private int timeout = 5000;
    private int maxMessageSize = 16 * 1024 * 1024;

    private DefaultMQProducer producer;

    public RocketProducer(String group, String nameSrv, String instanceName, String topic) {
        this.group = group;
        this.nameSrv = nameSrv;
        this.instanceName = instanceName;
        this.topic = topic;
        init();
    }

    public RocketProducer(Builder builder) {
        this.group = builder.group;
        this.nameSrv = builder.nameSrv;
        this.instanceName = builder.instanceName;
        this.topic = builder.topic;
        this.timeout = builder.timeout;
        this.maxMessageSize = builder.maxMessageSize;
        init();
    }

    private void init() {
        producer = new DefaultMQProducer(group);
        producer.setNamesrvAddr(nameSrv);
        producer.setSendMsgTimeout(timeout);
        producer.setInstanceName(instanceName);
        producer.setMaxMessageSize(maxMessageSize);
    }

    public boolean send(String data) throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        return send(data, "");
    }

    public boolean send(String data, String tag) throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        SendResult result = producer.send(new Message(topic, tag, data.getBytes()));
        return result != null ? result.getSendStatus() == SendStatus.SEND_OK : Boolean.FALSE;
    }

    public RocketProducer start() throws MQClientException {
        producer.start();
        return this;
    }

    public RocketProducer stop() {
        producer.shutdown();
        return this;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String group;
        private String nameSrv;
        private String instanceName;
        private String topic;
        private int timeout = 5000;
        private int maxMessageSize = 16 * 1024 * 1024;

        public RocketProducer build() {
            return new RocketProducer(this);
        }

        /**
         *
         * @param group RocketProducer 组名
         * @return
         */
        public Builder group(String group) {
            this.group = group;
            return this;
        }

        /**
         *
         * @param nameSrv RocketProducer nameSrvAddr
         * @return
         */
        public Builder nameSrv(String nameSrv) {
            this.nameSrv = nameSrv;
            return this;
        }

        /**
         *
         * @param instanceName RocketProducer instanceName
         * @return
         */
        public Builder instanceName(String instanceName) {
            this.instanceName = instanceName;
            return this;
        }

        /**
         *
         * @param topic RocketProducer topic
         * @return
         */
        public Builder topic(String topic) {
            this.topic = topic;
            return this;
        }

        /**
         *
         * @param timeout RocketProducer timeout 默认 5000 ms
         * @return
         */
        public Builder timeout(int timeout) {
            this.timeout = timeout;
            return this;
        }

        /**
         *
         * @param maxMessageSize RocketProducer maxMessageSize 默认16 * 1024 * 1024
         * @return
         */
        public Builder maxMessageSize(int maxMessageSize) {
            this.maxMessageSize = maxMessageSize;
            return this;
        }
    }
}
