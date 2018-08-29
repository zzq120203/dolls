package cn.ac.iie.dolls.mq

import org.apache.rocketmq.client.producer.DefaultMQProducer
import org.apache.rocketmq.client.producer.SendStatus
import org.apache.rocketmq.common.message.Message
import java.net.InetAddress

class RocketProducer (
        private val pGroup: String,
        private val pNameSrv: String,
        private val pInstanceName: String,
        private val topic: String,
        private val timeout: Int = 5 * 1000,
        private val pMaxMessageSize: Int = 16 * 1024 * 1024
) {
    private val producer: DefaultMQProducer by lazy {
        DefaultMQProducer(pGroup).apply {
            namesrvAddr = pNameSrv
            sendMsgTimeout = timeout
            instanceName = InetAddress.getLocalHost().hostAddress + pInstanceName
            maxMessageSize = pMaxMessageSize
        }
    }

    @JvmOverloads
    fun send(data: String, tag: String = ""): Boolean {
        val result = producer.send(Message(topic, tag, data.toByteArray()))
        return result?.sendStatus == SendStatus.SEND_OK
    }

    fun start() {
        producer.start()
    }

    fun stop() = producer.shutdown()
}