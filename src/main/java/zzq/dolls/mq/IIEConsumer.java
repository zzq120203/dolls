package zzq.dolls.mq;

import cn.ac.iie.di.datadock.rdata.exchange.client.core.session.receive.REAbstractReceiveMessageHandler;
import cn.ac.iie.di.datadock.rdata.exchange.client.exception.REConnectionException;
import cn.ac.iie.di.datadock.rdata.exchange.client.v1.ConsumePosition;
import cn.ac.iie.di.datadock.rdata.exchange.client.v1.FailureMessage;
import cn.ac.iie.di.datadock.rdata.exchange.client.v1.REMessageExt;
import cn.ac.iie.di.datadock.rdata.exchange.client.v1.connection.REConnection;
import cn.ac.iie.di.datadock.rdata.exchange.client.v1.session.FormattedHandler;
import cn.ac.iie.di.datadock.rdata.exchange.client.v1.session.REReceiveSession;
import cn.ac.iie.di.datadock.rdata.exchange.client.v1.session.REReceiveSessionBuilder;

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
    private String nameSrv;
    private String topic;
    private int threadNum = 1;

    private REReceiveSession receiver;

    private REConnection conn;

    private REReceiveSessionBuilder builder;

    private IIEConsumer(Builder builder) throws REConnectionException {
        this(builder.group, builder.nameSrv, builder.topic, builder.threadNum);
    }

    private IIEConsumer(String group, String nameSrv, String topic, int threadNum) throws REConnectionException {
        this.group = group;
        this.nameSrv = nameSrv;
        this.topic = topic;
        this.threadNum = threadNum;
        init();
    }

    private void init() throws REConnectionException {
        conn = new REConnection(nameSrv);
        builder = (REReceiveSessionBuilder) conn.getReceiveSessionBuilder(topic);
        builder.setGroupName(group);
        builder.setConsumPosition(ConsumePosition.CONSUME_FROM_FIRST_OFFSET);
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
                                if (int.class.isAssignableFrom(field.getType())) {
                                    field.set(d, rec.getInt(field.getName()));
                                } else if (long.class.isAssignableFrom(field.getType())) {
                                    field.set(d, rec.getLong(field.getName()));
                                } else if (double.class.isAssignableFrom(field.getType())) {
                                    field.set(d, rec.getDouble(field.getName()));
                                } else if (String.class.isAssignableFrom(field.getType())) {
                                    field.set(d, rec.getString(field.getName()));
                                } else if (boolean.class.isAssignableFrom(field.getType())) {
                                    field.set(d, rec.getBoolean(field.getName()));
                                } else if (List.class.isAssignableFrom(field.getType())) {
                                    Type generic = field.getGenericType();
                                    if (generic == null)
                                        throw new RuntimeException("unknown data type exception -> " + field.getType());
                                    if (generic instanceof ParameterizedType) {
                                        ParameterizedType pt = (ParameterizedType) generic;
                                        Class gc = (Class) pt.getActualTypeArguments()[0];
                                        if (Integer.class.isAssignableFrom(gc)) {
                                            field.set(d, rec.getInts(field.getName()));
                                        } else if (Long.class.isAssignableFrom(gc)) {
                                            field.set(d, rec.getLongs(field.getName()));
                                        } else if (Double.class.isAssignableFrom(gc)) {
                                            field.set(d, rec.getDoubles(field.getName()));
                                        } else if (String.class.isAssignableFrom(gc)) {
                                            field.set(d, rec.getStrings(field.getName()));
                                        } else if (Boolean.class.isAssignableFrom(gc)) {
                                            field.set(d, rec.getBooleans(field.getName()));
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
        conn.close();
        return this;
    }

    public static final class Builder {
        private String group;
        private String nameSrv;
        private String topic;
        private int threadNum = 1;

        public IIEConsumer build() throws REConnectionException {
            return new IIEConsumer(this);
        }

        public Builder group(String group) {
            this.group = group;
            return this;
        }

        public Builder nameSrv(String nameSrv) {
            this.nameSrv = nameSrv;
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

    }
}
