package zzq.dolls.mq.iie;

import cn.ac.iie.di.datadock.rdata.exchange.client.core.REFieldType;
import cn.ac.iie.di.datadock.rdata.exchange.client.exception.REConnectionException;
import cn.ac.iie.di.datadock.rdata.exchange.client.exception.RESessionException;
import cn.ac.iie.di.datadock.rdata.exchange.client.v1.connection.REConnection;
import cn.ac.iie.di.datadock.rdata.exchange.client.v1.session.*;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializer;
import com.google.gson.annotations.SerializedName;
import com.google.gson.reflect.TypeToken;
import zzq.dolls.config.From;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * iie 消息队列
 * @author zhangzhanqi
 */
public class IIEProducer {
    private static Gson gson = new GsonBuilder().registerTypeAdapter(new TypeToken<Map<String, Object>>() {
    }.getType(), (JsonDeserializer<Map<String, Object>>) (json, typeOfT, context) -> {
        Map<String, Object> map = json.getAsJsonObject().entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        return map;
    }).create();

    private String schemaName;
    private String user;
    private String password;
    private String round;

    private HashMap<String, String> user_desc = new HashMap<>();

    private int packageSize;

    private RESendSession sender;

    private REConnection conn;

    private RESendSessionBuilder builder;

    private boolean handler = true;

    private Class c;

    private IIEProducer(Builder builder) {
        this(builder.conn, builder.builder, builder.schemaName, builder.user, builder.password, builder.round, builder.packageSize, builder.c);
    }

    public IIEProducer(REConnection conn, RESendSessionBuilder builder, String schemaName, String user, String password, String round, int packageSize, Class c) {
        this.conn = conn;
        this.builder = builder;
        this.schemaName = schemaName;
        this.user = user;
        this.password = password;
        this.round = round;
        this.packageSize = packageSize;
        this.c = c;
    }

    public <Data> boolean send(Data data) throws RESessionException {
        return send(gson.toJson(data));
    }

    public boolean send(String data) throws RESessionException {
        Map map = gson.fromJson(data, Map.class);
        for (Field field : c.getDeclaredFields()) {
            field.setAccessible(true);
            String name = IIEClient.getFieldName(field);
            if (map.containsKey(name)) continue;
            String value = map.get(name).toString();
            try {
                if (int.class.isAssignableFrom(field.getType())) {
                    sender.setInt(name, Integer.parseInt(value));
                } else if (long.class.isAssignableFrom(field.getType())) {
                    sender.setLong(name, Long.parseLong(value));
                } else if (double.class.isAssignableFrom(field.getType())) {
                    sender.setDouble(name, Double.parseDouble(value));
                } else if (String.class.isAssignableFrom(field.getType())) {
                    sender.setString(name, value);
                } else if (boolean.class.isAssignableFrom(field.getType())) {
                    sender.setBoolean(name, Boolean.parseBoolean(value));
                } else if (List.class.isAssignableFrom(field.getType())) {
                    Type generic = field.getGenericType();
                    if(generic == null) throw new RuntimeException("unknown data type exception -> " + field.getType());
                    if(generic instanceof ParameterizedType) {
                        ParameterizedType pt = (ParameterizedType) generic;
                        Class gc = (Class)pt.getActualTypeArguments()[0];
                        if (Integer.class.isAssignableFrom(gc))
                            sender.setInts(name, gson.fromJson(value, new TypeToken<ArrayList<Integer>>(){}.getType()));
                        else if (Long.class.isAssignableFrom(gc)) {
                            sender.setLongs(name, gson.fromJson(value, new TypeToken<ArrayList<Long>>(){}.getType()));
                        } else if (Double.class.isAssignableFrom(gc)) {
                            sender.setDoubles(name, gson.fromJson(value, new TypeToken<ArrayList<Double>>(){}.getType()));
                        } else if (String.class.isAssignableFrom(gc)) {
                            sender.setStrings(name, gson.fromJson(value, new TypeToken<ArrayList<String>>(){}.getType()));
                        } else if (Boolean.class.isAssignableFrom(gc)) {
                            sender.setBooleans(name, gson.fromJson(value, new TypeToken<ArrayList<Boolean>>(){}.getType()));
                        }
                    }
                } else {
                    throw new RuntimeException("unknown data type exception -> " + field.getType());
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        sender.add();
        return true;
    }

    public IIEProducer start() throws REConnectionException, RESessionException {
        sender = (RESendSession) builder.build();
        sender.start();
        if (schemaName != null) {
            sender.setDocSchemaName(schemaName);
        }
        if (user != null && password != null) {
            user_desc.put("user", user);
            user_desc.put("pwd", password);
            user_desc.put("round", round);
        }
        Thread t = new Thread(() -> {
            while (handler) {
                if (sender.getCurrentDataSize() >= packageSize) {
                    try {
                        flush();
                    } catch (RESessionException | REConnectionException e) {
                        e.printStackTrace();
                    }
                }
            }
        }, "SendHandler");
        t.start();
        return this;
    }

    private void flush() throws RESessionException, REConnectionException {
        if (!user_desc.isEmpty()) {
            sender.setUserDesc(user_desc);
        }
        sender.flush();
    }

    public IIEProducer stop() throws REConnectionException, RESessionException {
        handler = false;
        flush();
        sender.shutdown();
        return this;
    }

    public static final class Builder {
        private String topic;

        private String schemaName;
        private String user;
        private String password;
        private String round = "all";

        private int packageSize = 100;

        private REConnection conn;

        private RESendSessionBuilder builder;

        private Class c;

        public Builder(REConnection conn) {
            this.conn = conn;
        }

        public <Data> Builder data(DataFactory<Data> df) {
            c = df.newInstance().getClass();
            return this;
        }

        public IIEProducer build() throws REConnectionException {
            builder = (RESendSessionBuilder) conn.getSendSessionBuilder(topic);
            for (Field field : c.getDeclaredFields()) {
                field.setAccessible(true);
                String name = IIEClient.getFieldName(field);
                try {
                    if (int.class.isAssignableFrom(field.getType())) {
                        builder.addColumn(name, REFieldType.Int, true);
                    } else if (long.class.isAssignableFrom(field.getType())) {
                        builder.addColumn(name, REFieldType.Long, true);
                    } else if (double.class.isAssignableFrom(field.getType())) {
                        builder.addColumn(name, REFieldType.Double, true);
                    } else if (String.class.isAssignableFrom(field.getType())) {
                        builder.addColumn(name, REFieldType.String, true);
                    } else if (boolean.class.isAssignableFrom(field.getType())) {
                        builder.addColumn(name, REFieldType.Boolean, true);
                    } else if (List.class.isAssignableFrom(field.getType())) {
                        Type generic = field.getGenericType();
                        if(generic == null) throw new RuntimeException("unknown data type exception -> " + field.getType());
                        if(generic instanceof ParameterizedType) {
                            ParameterizedType pt = (ParameterizedType) generic;
                            Class gc = (Class)pt.getActualTypeArguments()[0];
                            if (Integer.class.isAssignableFrom(gc)) {
                                builder.addColumn(name, REFieldType.Ints, true);
                            } else if (Long.class.isAssignableFrom(gc)) {
                                builder.addColumn(name, REFieldType.Longs, true);
                            } else if (Double.class.isAssignableFrom(gc)) {
                                builder.addColumn(name, REFieldType.Doubles, true);
                            } else if (String.class.isAssignableFrom(gc)) {
                                builder.addColumn(name, REFieldType.Strings, true);
                            } else if (Boolean.class.isAssignableFrom(gc)) {
                                builder.addColumn(name, REFieldType.Booleans, true);
                            }
                        }
                    } else {
                        throw new RuntimeException("unknown data type exception -> " + field.getType());
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            return new IIEProducer(this);
        }

        public Builder topic(String topic) {
            this.topic = topic;
            return this;
        }

        public Builder packageSize(int packageSize) {
            this.packageSize = packageSize;
            return this;
        }

        public Builder schemaName(String schemaName) {
            this.schemaName = schemaName;
            return this;
        }

        public Builder user(String user) {
            this.user = user;
            return this;
        }

        public Builder password(String password) {
            this.password = password;
            return this;
        }

        public Builder round(String round) {
            this.round = round;
            return this;
        }

    }
}
