package demo

import cn.ac.iie.dolls.mq.RocketConsumer
import cn.ac.iie.dolls.mq.RocketProducer

fun main(args: Array<String>) {
    val producer = RocketProducer("group", "nameSrv", "instanceName", "topic", 1000, 1024)
    producer.start()
    producer.send("test")

    val consumer = RocketConsumer("group", "nameSrv", "instanceName", "topic", "tag", 1, 10)
    consumer.message {
        println(body.toString(Charsets.UTF_8))
    }
    consumer.start()
}