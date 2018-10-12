package demo

import zzq.dolls.mq.RocketConsumer
import zzq.dolls.mq.RocketProducer

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