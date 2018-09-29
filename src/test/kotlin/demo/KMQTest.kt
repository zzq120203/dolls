package demo

import cn.ac.iie.dolls.mq.RocketConsumer
import cn.ac.iie.dolls.mq.RocketProducer

fun main(args: Array<String>) {
    val producer = RocketProducer("group", "nameSrv", "instanceName", "topic")
    producer.start()
    producer.send("test")

    val consumer = RocketConsumer("group", "nameSrv", "instanceName", "topic")
    consumer.message { msg ->
        println(msg.body.toString(Charsets.UTF_8))
    }
    consumer.start()
}