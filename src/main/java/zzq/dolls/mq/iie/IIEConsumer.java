package zzq.dolls.mq.iie;

import cn.ac.iie.di.datadock.rdata.exchange.client.core.session.receive.REAbstractReceiveMessageHandler;
import cn.ac.iie.di.datadock.rdata.exchange.client.exception.REConnectionException;
import cn.ac.iie.di.datadock.rdata.exchange.client.v1.ConsumePosition;
import cn.ac.iie.di.datadock.rdata.exchange.client.v1.FailureMessage;
import cn.ac.iie.di.datadock.rdata.exchange.client.v1.REMessageExt;
import cn.ac.iie.di.datadock.rdata.exchange.client.v1.connection.REConnection;
import cn.ac.iie.di.datadock.rdata.exchange.client.v1.session.FormattedHandler;
import cn.ac.iie.di.datadock.rdata.exchange.client.v1.session.REReceiveSession;
import cn.ac.iie.di.datadock.rdata.exchange.client.v1.session.REReceiveSessionBuilder;
import com.google.gson.annotations.SerializedName;
import zzq.dolls.config.From;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

/**
 * iie 消息队列
 * @author zhangzhanqi
 */
public class IIEConsumer {

    private String group;
    private String topic;
    private int threadNum;
    private ConsumePosition consumeFromWhere;
    private long consumeTimestamp;

    private REReceiveSession receiver;

    private REReceiveSessionBuilder builder;

    private IIEConsumer(Builder builder) throws REConnectionException {
        this(builder.conn, builder.group, builder.topic, builder.threadNum, builder.consumeFromWhere, builder.consumeTimestamp);
    }

    private IIEConsumer(REConnection conn, String group, String topic, int threadNum, ConsumePosition consumeFromWhere, long consumeTimestamp) throws REConnectionException {
        this.group = group;
        this.topic = topic;
        this.threadNum = threadNum;
        this.consumeFromWhere = consumeFromWhere;
        this.consumeTimestamp = consumeTimestamp;
        init(conn);
    }

    private void init(REConnection conn) throws REConnectionException {
        builder = (REReceiveSessionBuilder) conn.getReceiveSessionBuilder(topic);
        builder.setGroupName(group);
        builder.setConsumPosition(consumeFromWhere);
        if (consumeFromWhere == ConsumePosition.CONSUME_FROM_TIMESTAMP) builder.setConsumeTimestamp(consumeTimestamp);
        builder.setConsumeThreadNum(threadNum);
        builder.setFailureHandler(new REAbstractReceiveMessageHandler<FailureMessage>() {
            @Override
            public boolean handle(FailureMessage failureMessage) {
                return false;
            }
        });

    }

    public <Data> void  message(DataFactory<Data> df, Function<Data, Boolean> fun) throws REConnectionException {
        Data d = df.newInstance();
        builder.setHandler(new FormattedHandler() {
            @Override
            public boolean handle(REMessageExt reMessageExt) {
                Iterator<REMessageExt.Record> itr =  reMessageExt.getRecordIterator();
                while (itr.hasNext()) {
                    REMessageExt.Record rec = itr.next();
                    try {
                        for (Field field : d.getClass().getDeclaredFields()) {
                            field.setAccessible(true);
                            String name = IIEClient.getFieldName(field);
                            if (int.class.isAssignableFrom(field.getType())) {
                                field.set(d, rec.getInt(name));
                            } else if (long.class.isAssignableFrom(field.getType())) {
                                field.set(d, rec.getLong(name));
                            } else if (double.class.isAssignableFrom(field.getType())) {
                                field.set(d, rec.getDouble(name));
                            } else if (String.class.isAssignableFrom(field.getType())) {
                                field.set(d, rec.getString(name));
                            } else if (boolean.class.isAssignableFrom(field.getType())) {
                                field.set(d, rec.getBoolean(name));
                            } else if (List.class.isAssignableFrom(field.getType())) {
                                Type generic = field.getGenericType();
                                if (generic == null)
                                    throw new RuntimeException("unknown data type exception -> " + field.getType());
                                if (generic instanceof ParameterizedType) {
                                    ParameterizedType pt = (ParameterizedType) generic;
                                    Class gc = (Class) pt.getActualTypeArguments()[0];
                                    if (Integer.class.isAssignableFrom(gc)) {
                                        field.set(d, rec.getInts(name));
                                    } else if (Long.class.isAssignableFrom(gc)) {
                                        field.set(d, rec.getLongs(name));
                                    } else if (Double.class.isAssignableFrom(gc)) {
                                        field.set(d, rec.getDoubles(name));
                                    } else if (String.class.isAssignableFrom(gc)) {
                                        field.set(d, rec.getStrings(name));
                                    } else if (Boolean.class.isAssignableFrom(gc)) {
                                        field.set(d, rec.getBooleans(name));
                                    }
                                }
                            } else {
                                throw new RuntimeException("unknown data type exception -> " + field.getType());
                            }
                        }
                        fun.apply(d);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                return true;
            }
        });
        receiver = (REReceiveSession) builder.build();
    }

    public <T> void  message(Function<REMessageExt.Record, T> fun) throws REConnectionException {
        builder.setHandler(new FormattedHandler() {
            @Override
            public boolean handle(REMessageExt reMessageExt) {
                Iterator<REMessageExt.Record> itr =  reMessageExt.getRecordIterator();
                while (itr.hasNext()) {
                    REMessageExt.Record rec = itr.next();
                    fun.apply(rec);
                }
                return true;
            }
        });
        receiver = (REReceiveSession) builder.build();
    }

    public IIEConsumer start() throws REConnectionException {
        receiver.start();
        return this;
    }

    public IIEConsumer stop() throws REConnectionException {
        receiver.shutdown();
        return this;
    }

    public static final class Builder {

        private String group;
        private String topic;
        private int threadNum = 1;

        private ConsumePosition consumeFromWhere = ConsumePosition.CONSUME_FROM_FIRST_OFFSET;
        private long consumeTimestamp = 0L;

        private REConnection conn;

        public Builder(REConnection conn) {
            this.conn = conn;
        }

        public IIEConsumer build() throws REConnectionException {
            return new IIEConsumer(this);
        }

        public Builder group(String group) {
            this.group = group;
            return this;
        }

        public Builder topic(String topic) {
            this.topic = topic;
            return this;
        }

        public Builder threadNum(int threadNum) {
            this.threadNum = threadNum;
            return this;
        }

        public Builder consumeFromWhere(ConsumePosition consumeFromWhere) {
            this.consumeFromWhere = consumeFromWhere;
            return this;
        }

        public Builder consumeTimestamp(long consumeTimestamp) {
            this.consumeTimestamp = consumeTimestamp;
            return this;
        }

    }
}
