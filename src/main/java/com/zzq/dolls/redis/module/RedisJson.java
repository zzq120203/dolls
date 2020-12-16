package com.zzq.dolls.redis.module;

import com.google.gson.Gson;
import com.redislabs.modules.rejson.JReJSON;
import com.redislabs.modules.rejson.Path;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.commands.ProtocolCommand;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.util.Pool;
import redis.clients.jedis.util.SafeEncoder;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

public class RedisJson extends JReJSON {

    private static final Gson gson = new Gson();

    private final Pool<Jedis> client;


    public RedisJson(Pool<Jedis> jedis) {
        super(jedis);
        this.client = jedis;
    }

    private enum Command implements ProtocolCommand {
        GET("JSON.GET"),
        ARRAPPEND("JSON.ARRAPPEND"),
        STRAPPEND("JSON.STRAPPEND"),
        OBJLEN("JSON.OBJLEN"),
        SET("JSON.SET");
        private final byte[] raw;

        Command(String alt) {
            raw = SafeEncoder.encode(alt);
        }

        public byte[] getRaw() {
            return raw;
        }
    }

    /**
     * Gets an object
     * @param key the key name
     * @param type
     * @param paths optional one ore more paths in the object
     * @return the requested object
     */
    public <T> T get(String key, Type type, Path... paths) {
        byte[][] args = new byte[1 + paths.length][];
        int i=0;
        args[i] = SafeEncoder.encode(key);
        for (Path p :paths) {
            args[++i] = SafeEncoder.encode(p.toString());
        }

        String rep;
        try (Jedis conn = getConnection()) {
            conn.getClient().sendCommand(Command.GET, args);
            rep = conn.getClient().getBulkReply();
        }
        if (rep == null) {
            return null;
        }
        assertReplyNotError(rep);
        return gson.fromJson(rep, type);
    }

    public Boolean setnx(String key, Object object, Path path) {

        List<byte[]> args = new ArrayList<>(4);

        args.add(SafeEncoder.encode(key));
        args.add(SafeEncoder.encode(path.toString()));
        args.add(SafeEncoder.encode(gson.toJson(object)));
        args.add(ExistenceModifier.NOT_EXISTS.getRaw());

        String status;
        try (Jedis conn = getConnection()) {
            conn.getClient()
                    .sendCommand(Command.SET, args.toArray(new byte[args.size()][]));
            status = conn.getClient().getStatusCodeReply();
        }
        assertReplyOK(status);
        return status != null;
    }

    public void arrAppend(String key, Object object, Path path) {
        List<byte[]> args = new ArrayList<>(4);

        args.add(SafeEncoder.encode(key));
        args.add(SafeEncoder.encode(path.toString()));
        if (object instanceof List) {
            ((List<?>) object).forEach(obj ->
                    args.add(SafeEncoder.encode(gson.toJson(obj)))
            );
        } else {
            args.add(SafeEncoder.encode(gson.toJson(object)));
        }

        Object status;
        try (Jedis conn = getConnection()) {
            conn.getClient()
                    .sendCommand(Command.ARRAPPEND, args.toArray(new byte[args.size()][]));
            try {
                status = conn.getClient().getIntegerReply();
            } catch (JedisDataException e) {
                if (e.getMessage().startsWith("ERR key")) {
                    // arr is not exist , set

                    args.clear();
                    args.add(SafeEncoder.encode(key));
                    args.add(SafeEncoder.encode(path.toString()));
                    if (object instanceof List) {
                        args.add(SafeEncoder.encode(gson.toJson(object)));
                    } else {
                        List<Object> objects = new ArrayList<>();
                        objects.add(object);
                        args.add(SafeEncoder.encode(gson.toJson(objects)));
                    }
                    conn.getClient()
                            .sendCommand(Command.SET, args.toArray(new byte[args.size()][]));
                    status = conn.getClient().getStatusCodeReply();
                } else {
                    throw new JedisDataException(e.getMessage());
                }
            }
        }
        assertReplyNotError(status);
    }

    public void strAppend(String key, Object object, Path path) {

        List<byte[]> args = new ArrayList<>(3);

        args.add(SafeEncoder.encode(key));
        args.add(SafeEncoder.encode(path.toString()));
        if (object instanceof List) {
            ((List<?>) object).forEach(obj ->
                args.add(SafeEncoder.encode(gson.toJson(obj)))
            );
        } else {
            args.add(SafeEncoder.encode(gson.toJson(object)));
        }

        Object status;
        try (Jedis conn = getConnection()) {
            conn.getClient()
                    .sendCommand(Command.STRAPPEND, args.toArray(new byte[args.size()][]));
            try {
                status = conn.getClient().getIntegerReply();
            } catch (JedisDataException e) {
                if (e.getMessage().startsWith("ERR key")) {
                    // str is not exist , set
                    args.clear();
                    args.add(SafeEncoder.encode(key));
                    args.add(SafeEncoder.encode(path.toString()));
                    args.add(SafeEncoder.encode(gson.toJson(object)));
                    conn.getClient()
                            .sendCommand(Command.SET, args.toArray(new byte[args.size()][]));
                    status = conn.getClient().getStatusCodeReply();
                } else {
                    throw new JedisDataException(e.getMessage());
                }
            }
        }
        assertReplyOK(status);
    }

    public long objLen(String key) {
        return objLen(key, Path.ROOT_PATH);
    }

    public long objLen(String key, Path path) {

        List<byte[]> args = new ArrayList<>(2);

        args.add(SafeEncoder.encode(key));
        args.add(SafeEncoder.encode(path.toString()));

        Long status;
        try (Jedis conn = getConnection()) {
            conn.getClient()
                    .sendCommand(Command.OBJLEN, args.toArray(new byte[args.size()][]));
            status = conn.getClient().getIntegerReply();
        }
        return status == null ? -1 : status;
    }

    private Jedis getConnection() {
        return this.client.getResource();
    }

    /**
     * Helper to check for an OK reply
     * @param str the reply string to "scrutinize"
     */
    private static void assertReplyOK(final Object str) {
        if (str instanceof String && !str.toString().equals("OK"))
            throw new RuntimeException((String) str);
    }
    /**
     *  Helper to check for errors and throw them as an exception
     * @param str the reply string to "analyze"
     * @throws RuntimeException
     */
    private static void assertReplyNotError(final Object str) {
        if (str instanceof String && str.toString().startsWith("-ERR"))
            throw new RuntimeException(str.toString().substring(5));
    }


    public ModulePipeline pipelined() {
        ModulePipeline pipeline = new ModulePipeline();
        pipeline.setClient(getConnection().getClient());
        return pipeline;
    }

}
