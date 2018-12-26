package zzq.dolls.mq.iie;

import cn.ac.iie.di.datadock.rdata.exchange.client.exception.REConnectionException;
import cn.ac.iie.di.datadock.rdata.exchange.client.v1.connection.REConnection;

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
}
