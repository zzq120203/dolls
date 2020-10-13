package com.zzq.dolls.mq.rocket;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;
import java.util.function.Consumer;

public class RocketConsumer {

    private String group;
    private String nameSrv;
    private String instanceName;
    private String topic;
    private String tag = "*";
    private int threadMin = 1;
    private int threadMax = 1;

    private ConsumeFromWhere consumeFromWhere = ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET;

    private DefaultMQPushConsumer consumer;

    public RocketConsumer(String group, String nameSrv, String instanceName, String topic) {
        this.group = group;
        this.nameSrv = nameSrv;
        this.instanceName = instanceName;
        this.topic = topic;
        init();
    }

    /**
     * @param builder
     */
    public RocketConsumer(Builder builder) {
        this.group = builder.group;
        this.nameSrv = builder.nameSrv;
        this.instanceName = builder.instanceName;
        this.topic = builder.topic;
        this.tag = builder.tag;
        this.threadMin = builder.threadMin;
        this.threadMax = builder.threadMax;
        this.consumeFromWhere = builder.consumeFromWhere;
        init();
    }

    public <T> void message(Consumer<MessageExt> fun) {
        consumer.registerMessageListener((List<MessageExt> msgs, ConsumeConcurrentlyContext context) -> {
            fun.accept(msgs.get(0));
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
    }

    private void init() {
        if (consumer == null) {
            consumer = new DefaultMQPushConsumer(group);
            consumer.setNamesrvAddr(nameSrv);
            consumer.setConsumeFromWhere(consumeFromWhere);
            consumer.setInstanceName(instanceName);
            try {
                consumer.subscribe(topic, tag);
                consumer.setConsumeThreadMin(threadMin);
                consumer.setConsumeThreadMax(threadMax);
            } catch (MQClientException e) {
                e.printStackTrace();
            }
        }
    }

    public RocketConsumer start() throws MQClientException {
        consumer.start();
        return this;
    }

    public RocketConsumer stop() {
        consumer.shutdown();
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
        private String tag = "*";
        private int threadMin = 1;
        private int threadMax = 1;
        private ConsumeFromWhere consumeFromWhere = ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET;

        /**
         *
         * 通过Builder 创建RocketConsumer
         * @return RocketConsumer
         */
        public RocketConsumer build() {
            return new RocketConsumer(this);
        }

        /**
         *
         * @param group RocketConsumer 组名
         * @return
         */
        public Builder group(String group) {
            this.group = group;
            return this;
        }

        /**
         *
         * @param nameSrv RocketConsumer nameSrvAddr
         * @return
         */
        public Builder nameSrv(String nameSrv) {
            this.nameSrv = nameSrv;
            return this;
        }

        /**
         *
         * @param instanceName RocketConsumer instanceName
         * @return
         */
        public Builder instanceName(String instanceName) {
            this.instanceName = instanceName;
            return this;
        }

        /**
         *
         * @param topic RocketConsumer topic
         * @return
         */
        public Builder topic(String topic) {
            this.topic = topic;
            return this;
        }

        /**
         *
         * @param tag RocketConsumer tag 默认 *
         * @return
         */
        public Builder tag(String tag) {
            this.tag = tag;
            return this;
        }

        /**
         *
         * @param threadMin 最小消费线程 默认 1
         * @return
         */
        public Builder threadMin(int threadMin) {
            this.threadMin = threadMin;
            return this;
        }

        /**
         *
         * @param threadMax 最大线程数 默认 1
         * @return
         */
        public Builder threadMax(int threadMax) {
            this.threadMax = threadMax;
            return this;
        }

        /**
         *
         * @param consumeFromWhere RocketConsumer consumeFromWhere 默认 CONSUME_FROM_FIRST_OFFSET
         * @see ConsumeFromWhere
         * @return
         */
        public Builder consumeFromWhere(ConsumeFromWhere consumeFromWhere) {
            this.consumeFromWhere = consumeFromWhere;
            return this;
        }


    }
}
