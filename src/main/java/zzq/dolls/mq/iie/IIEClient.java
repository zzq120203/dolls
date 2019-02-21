package zzq.dolls.mq.iie;

import cn.ac.iie.di.datadock.rdata.exchange.client.exception.REConnectionException;
import cn.ac.iie.di.datadock.rdata.exchange.client.v1.connection.REConnection;
import com.google.gson.annotations.SerializedName;
import zzq.dolls.config.From;

import java.lang.reflect.Field;

public class IIEClient {

    private REConnection conn;

    private IIEClient(Builder builder) {
        conn = new REConnection(builder.url);
    }

    public IIEConsumer.Builder newConsumer() {
        return new IIEConsumer.Builder(conn);
    }

    public IIEProducer.Builder newProducer() {
        return new IIEProducer.Builder(conn);
    }

    public void close() throws REConnectionException {
        this.conn.close();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String url;

        public Builder url(String url) {
            this.url = url;
            return this;
        }

        public IIEClient build() {
            return new IIEClient(this);
        }
    }

    static String getFieldName(Field field) {
        zzq.dolls.config.From from = field.getAnnotation(From.class);
        if (from != null) {
            return from.value().toLowerCase();
        }
        SerializedName serializedName = field.getAnnotation(SerializedName.class);
        if (serializedName != null) {
            return serializedName.value().toLowerCase();
        }
        return field.getName().toLowerCase();
    }
}
