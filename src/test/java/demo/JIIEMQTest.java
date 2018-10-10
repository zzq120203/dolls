package demo;

import cn.ac.iie.di.datadock.rdata.exchange.client.exception.REConnectionException;
import cn.ac.iie.di.datadock.rdata.exchange.client.exception.RESessionException;
import cn.ac.iie.dolls.mq.IIEConsumer;
import cn.ac.iie.dolls.mq.IIEProducer;
import com.google.gson.Gson;

import java.util.List;

public class JIIEMQTest {

    private static Gson gson = new Gson();

    public static void main(String[] args) throws REConnectionException, RESessionException {

        IIEProducer pro = new IIEProducer.Builder<Data>()
                .nameSrv("")
                .topic("")
                .user("")
                .password("")
                .packageSize(10)
                .build();

        pro.send(gson.toJson(new Data()));


        IIEConsumer con = new IIEConsumer.Builder()
                .group("group")
                .nameSrv("nameSrv")
                .topic("topic")
                .threadNum(10)
                .build();
        con.message(messageExt -> {
            try {
                String str = messageExt.getString("str1");
                System.out.print(str);
                List<String> list = messageExt.getStrings("list1");
                System.out.print(list.toString());
            } catch (RESessionException e) {
                e.printStackTrace();
            }
            return true;
        });
        con.start();

    }
}

class Data {
    private String str1;

    private List<Long> list;

    public String getStr1() {
        return str1;
    }

    public void setStr1(String str1) {
        this.str1 = str1;
    }

    public List<Long> getList() {
        return list;
    }

    public void setList(List<Long> list) {
        this.list = list;
    }
}
