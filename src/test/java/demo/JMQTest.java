package demo;

import cn.ac.iie.dolls.mq.RocketConsumer;
import cn.ac.iie.dolls.mq.RocketProducer;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.Arrays;

public class JMQTest {

    public static void main(String[] args) {

        RocketProducer pro = new RocketProducer.Builder()
                .group("")
                .nameSrv("")
                .instanceName("")
                .topic("")
                .build();
        try {
            pro.start();
            pro.send("test");
            pro.send("test", "tag");
        } catch (MQClientException | InterruptedException | RemotingException | MQBrokerException e) {
            e.printStackTrace();
        }


        RocketConsumer con = new RocketConsumer.Builder()
                .group("")
                .nameSrv("")
                .instanceName("")
                .topic("")
                .build();
        con.message(messageExt -> {
            byte[] body = messageExt.getBody();
            System.out.print(Arrays.toString(body));
            return true;
        });
        try {
            con.start();
        } catch (MQClientException e) {
            e.printStackTrace();
        }
    }
}
