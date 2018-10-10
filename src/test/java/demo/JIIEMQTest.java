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

        IIEProducer pro = new IIEProducer.Builder()
                .nameSrv("nameSrv")//必要
                .topic("topic")//必要
                .user("user")
                .password("password")
                .packageSize(1000)//发送包大小，默认100
                .data(Data::new)//必要
                .build();

        pro.send(gson.toJson(new Data()));


        IIEConsumer con = new IIEConsumer.Builder()
                .nameSrv("10.136.64.28:9877;10.136.64.29:9877;10.136.64.30:9877;10.136.64.31:9877;10.136.64.32:9877;10.136.16.46:9877")//必要
                .group("zzq-wb")//必要
                .topic("swb_msg_pic_mq")//必要
                .threadNum(2)
                .build();
        con.message(Data::new, data -> {//不支持Map(Struct)
            System.out.println(data.getCont());
            return true;
        });
        con.message(data -> {//支持Map(Struct)
            try {
                List<String> list = data.getStrings("wb_lpic_name");
                String asp = data.getString("asp");
                Long pt = data.getLong("pt");
                String uid = data.getString("uid");
                String cont = data.getString("cont");
                System.out.println(list.toString());
                System.out.println(asp);
                System.out.println(pt);
                System.out.println(uid);
                System.out.println(cont);
            } catch (RESessionException e) {
                e.printStackTrace();
            }
            return true;
        });
        con.start();

    }
}

class Data {
    public String getCont() {
        return cont;
    }

    public void setCont(String cont) {
        this.cont = cont;
    }

    public List<String> getWb_lpic_name() {
        return wb_lpic_name;
    }

    public void setWb_lpic_name(List<String> wb_lpic_name) {
        this.wb_lpic_name = wb_lpic_name;
    }

    private String cont;

    private List<String> wb_lpic_name;

    @Override
    public String toString() {
        return "Data{" +
                "cont='" + cont + '\'' +
                ", wb_lpic_name=" + wb_lpic_name +
                '}';
    }
}
