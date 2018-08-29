package demo;

import cn.ac.iie.dolls.mq.RocketConsumer;
import cn.ac.iie.dolls.mq.RocketProducer;

import java.util.Arrays;

public class JMQTest {

    public static void main(String[] args) {

        RocketProducer pro = new RocketProducer("group", "nameSrv", "instanceName" ,"topic", 1000, 1024);
        pro.start();
        pro.send("test");
        pro.send("test", "tag");


        RocketConsumer con = new RocketConsumer("group", "nameSrv", "instanceName" ,"topic", "tag", 1, 10);
        con.message(messageExt -> {
            byte[] body = messageExt.getBody();
            System.out.print(Arrays.toString(body));
            return null;//函数需要返回值
        });
        con.start();
    }
}
