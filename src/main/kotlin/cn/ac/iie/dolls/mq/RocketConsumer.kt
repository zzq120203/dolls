package cn.ac.iie.dolls.mq

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus
import org.apache.rocketmq.common.consumer.ConsumeFromWhere
import org.apache.rocketmq.common.message.MessageExt
import java.net.InetAddress

class RocketConsumer(
        private val cGroup: String,
        private val cNameSrv: String,
        private val cInstanceName: String,
        private val topic: String,
        private val tag: String = "*",
        private val cThreadMin: Int = 1,
        private val cThreadMax: Int = 1
) {
    private val consumer: DefaultMQPushConsumer by lazy {
        DefaultMQPushConsumer(cGroup).apply {
            namesrvAddr = cNameSrv
            consumeFromWhere = ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET
            instanceName = InetAddress.getLocalHost().hostAddress + cInstanceName
            subscribe(topic, tag)
            consumeThreadMin = cThreadMin
            consumeThreadMax = cThreadMax
        }
    }

    fun <T> message(getFun: MessageExt.() -> T) {
        consumer.registerMessageListener { msgs: List<MessageExt>, _: ConsumeConcurrentlyContext ->
            msgs[0].getFun()
            return@registerMessageListener ConsumeConcurrentlyStatus.CONSUME_SUCCESS
        }
    }

    fun start() = consumer.start()

    fun stop() = consumer.shutdown()
}