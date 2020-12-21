package com.zzq.dolls.redis.module.json;

import com.google.gson.Gson;
import com.zzq.dolls.redis.exception.KeyNotFoundException;
import com.zzq.dolls.redis.module.ModulePipeline;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.commands.ProtocolCommand;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.util.Pool;
import redis.clients.jedis.util.SafeEncoder;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

public class RedisJson implements Json {

    private static final Gson gson = new Gson();

    private final Pool<Jedis> client;

    private enum Command implements ProtocolCommand {
        ARRAPPEND("JSON.ARRAPPEND"),
        STRAPPEND("JSON.STRAPPEND"),
        OBJLEN("JSON.OBJLEN"),
        DEL("JSON.DEL"),
        GET("JSON.GET"),
        SET("JSON.SET"),
        TYPE("JSON.TYPE");
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
    @Override
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

    @Override
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

    @Override
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
                    try {
                        conn.getClient()
                                .sendCommand(Command.SET, args.toArray(new byte[args.size()][]));
                        status = conn.getClient().getStatusCodeReply();
                    } catch (JedisDataException e1) {
                        if (e1.getMessage().startsWith("ERR missing")) {
                            throw new KeyNotFoundException(e.getMessage());
                        }
                        throw new JedisDataException(e.getMessage());
                    }
                } else {
                    throw new JedisDataException(e.getMessage());
                }
            }
        }
        assertReplyNotError(status);
    }

    @Override
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
                    try {
                        conn.getClient()
                                .sendCommand(Command.SET, args.toArray(new byte[args.size()][]));
                        status = conn.getClient().getStatusCodeReply();
                    } catch (JedisDataException e1) {
                        if (e1.getMessage().startsWith("ERR missing")) {
                            throw new KeyNotFoundException(e.getMessage());
                        }
                        throw new JedisDataException(e.getMessage());
                    }
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



    /**
     * Creates a client to the specific host/post
     *
     * @param host Redis host
     * @param port Redis port
     */
    public RedisJson(String host, int port) {
        this(new JedisPool(host, port));
    }

    /**
     * Creates a client using provided Jedis pool
     *
     * @param jedis bring your own Jedis pool
     */
    public RedisJson(Pool<Jedis> jedis) {
        this.client = jedis;
    }

    /**
     * Helper to handle single optional path argument situations
     * @param path a single optional path
     * @return the provided path or root if not
     */
    private static Path getSingleOptionalPath(Path... path) {
        // check for 0, 1 or more paths
        if (1 > path.length) {   // default to root
            return Path.RootPath();
        }
        if (1 == path.length) {  // take 1
            return path[0];
        }

        // throw out the baby with the water
        throw new RuntimeException("Only a single optional path is allowed");
    }

    /**
     * Deletes the root path
     * @param key the key name
     * @return the number of paths deleted (0 or 1)
     */
    @Override
    public Long del(String key) {
        return del(key, Path.ROOT_PATH);
    }


    /**
     * Deletes a path
     * @param key the key name
     * @param path optional single path in the object, defaults to root
     * @return path deleted
     */
    @Override
    public Long del(String key, Path path) {
        byte[][] args = new byte[2][];
        args[0] = SafeEncoder.encode(key);
        args[1] = SafeEncoder.encode(path.toString());

        try (Jedis conn = getConnection()) {
            conn.getClient().sendCommand(Command.DEL, args);
            return conn.getClient().getIntegerReply();
        }
    }

    /**
     * Gets an object at the root path
     * @param key the key name
     * @return the requested object
     */

    /**
     * Gets an object
     * @param key the key name
     * @param paths optional one ore more paths in the object
     * @return the requested object
     * @deprecated use {@link #get(String, Class, Path...)} instead
     */
    @Override
    @Deprecated
    public <T> T get(String key, Path... paths) {
        return (T)this.get(key, Object.class, paths);
    }

    /**
     * Gets an object
     * @param key the key name
     * @param clazz
     * @param paths optional one ore more paths in the object
     * @return the requested object
     */
    @Override
    public <T> T get(String key, Class<T> clazz, Path... paths) {
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
        assertReplyNotError(rep);
        return gson.fromJson(rep, clazz);
    }

    /**
     * Sets an object at the root path
     * @param key the key name
     * @param object the Java object to store
     * @param flag an existential modifier
     */
    @Override
    public void set(String key, Object object, ExistenceModifier flag) {
        set(key, object, flag, Path.ROOT_PATH);
    }

    /**
     * Sets an object in the root path
     * @param key the key name
     * @param object the Java object to store
     */
    @Override
    public void set(String key, Object object) {
        set(key, object, ExistenceModifier.DEFAULT, Path.ROOT_PATH);
    }

    /**
     * Sets an object without caring about target path existing
     * @param key the key name
     * @param object the Java object to store
     * @param path in the object
     */
    @Override
    public void set(String key, Object object, Path path) {
        set(key, object, ExistenceModifier.DEFAULT, path);
    }

    /**
     * Sets an object
     * @param key the key name
     * @param object the Java object to store
     * @param flag an existential modifier
     * @param path in the object
     * @return
     */
    @Override
    public void set(String key, Object object, ExistenceModifier flag, Path path) {

        List<byte[]> args = new ArrayList<>(4);

        args.add(SafeEncoder.encode(key));
        args.add(SafeEncoder.encode(path.toString()));
        args.add(SafeEncoder.encode(gson.toJson(object)));
        if (ExistenceModifier.DEFAULT != flag) {
            args.add(flag.getRaw());
        }

        String status;
        try (Jedis conn = getConnection()) {
            conn.getClient()
                    .sendCommand(Command.SET, args.toArray(new byte[args.size()][]));
            status = conn.getClient().getStatusCodeReply();
        }
        assertReplyOK(status);
    }

    /**
     * Gets the class of an object
     * @param key the key name
     * @param path a path in the object
     * @return the Java class of the requested object
     */
    @Override
    public Class<?> type(String key, Path path) {

        List<byte[]> args = new ArrayList<>(2);

        args.add(SafeEncoder.encode(key));
        args.add(SafeEncoder.encode(path.toString()));

        String rep;
        try (Jedis conn = getConnection()) {
            conn.getClient()
                    .sendCommand(Command.TYPE, args.toArray(new byte[args.size()][]));
            rep = conn.getClient().getBulkReply();
        }

        assertReplyNotError(rep);

        switch (rep) {
            case "null":
                return null;
            case "boolean":
                return boolean.class;
            case "integer":
                return int.class;
            case "number":
                return float.class;
            case "string":
                return String.class;
            case "object":
                return Object.class;
            case "array":
                return List.class;
            default:
                throw new java.lang.RuntimeException(rep);
        }
    }

    /**
     * Deletes a path
     * @param conn the Jedis connection
     * @param key the key name
     * @param path optional single path in the object, defaults to root
     * @return the number of paths deleted (0 or 1)
     * @deprecated use {@link #del(String, Path)} instead
     */
    protected static Long del(Jedis conn, String key, Path... path) {

        List<byte[]> args = new ArrayList<>(2);

        args.add(SafeEncoder.encode(key));
        args.add(SafeEncoder.encode(getSingleOptionalPath(path).toString()));

        conn.getClient()
                .sendCommand(Command.DEL, args.toArray(new byte[args.size()][]));
        Long rep = conn.getClient().getIntegerReply();

        return rep;
    }

    /**
     * Gets an object
     * @param conn the Jedis connection
     * @param key the key name
     * @param paths optional one ore more paths in the object, defaults to root
     * @return the requested object
     * @deprecated use {@link #get(String, Path...)} instead
     */
    protected static <T> T get(Jedis conn, String key, Class<T> clazz, Path... paths) {

        List<byte[]> args = new ArrayList<>(2);

        args.add(SafeEncoder.encode(key));
        for (Path p :paths) {
            args.add(SafeEncoder.encode(p.toString()));
        }

        conn.getClient()
                .sendCommand(Command.GET, args.toArray(new byte[args.size()][]));
        String rep = conn.getClient().getBulkReply();

        assertReplyNotError(rep);
        return gson.fromJson(rep, clazz);
    }

    /**
     * Sets an object
     * @param conn the Jedis connection
     * @param key the key name
     * @param object the Java object to store
     * @param flag an existential modifier
     * @param path optional single path in the object, defaults to root
     * @deprecated use {@link #set(String, Object, ExistenceModifier, Path)} instead
     */
    protected static void set(Jedis conn, String key, Object object, ExistenceModifier flag, Path... path) {

        List<byte[]> args = new ArrayList<>(4);

        args.add(SafeEncoder.encode(key));
        args.add(SafeEncoder.encode(getSingleOptionalPath(path).toString()));
        args.add(SafeEncoder.encode(gson.toJson(object)));
        if (ExistenceModifier.DEFAULT != flag) {
            args.add(flag.getRaw());
        }

        conn.getClient()
                .sendCommand(Command.SET, args.toArray(new byte[args.size()][]));
        String status = conn.getClient().getStatusCodeReply();

        assertReplyOK(status);
    }

    /**
     * Sets an object without caring about target path existing
     * @param conn the Jedis connection
     * @param key the key name
     * @param object the Java object to store
     * @param path optional single path in the object, defaults to root
     * @deprecated use {@link #set(String, Object, ExistenceModifier, Path)} instead
     */
    protected static void set(Jedis conn, String key, Object object, Path... path) {
        set(conn,key, object, ExistenceModifier.DEFAULT, path);
    }

    /**
     * Gets the class of an object
     * @param conn the Jedis connection
     * @param key the key name
     * @param path optional single path in the object, defaults to root
     * @return the Java class of the requested object
     * @deprecated use {@link #type(String, Path)} instead
     */
    protected static Class<?> type(Jedis conn, String key, Path... path) {

        List<byte[]> args = new ArrayList<>(2);

        args.add(SafeEncoder.encode(key));
        args.add(SafeEncoder.encode(getSingleOptionalPath(path).toString()));

        conn.getClient()
                .sendCommand(Command.TYPE, args.toArray(new byte[args.size()][]));
        String rep = conn.getClient().getBulkReply();

        assertReplyNotError(rep);

        switch (rep) {
            case "null":
                return null;
            case "boolean":
                return boolean.class;
            case "integer":
                return int.class;
            case "number":
                return float.class;
            case "string":
                return String.class;
            case "object":
                return Object.class;
            case "array":
                return List.class;
            default:
                throw new java.lang.RuntimeException(rep);
        }
    }

    /**
     * Gets an object
     * @param key the key name
     * @param type
     * @param paths optional one ore more paths in the object
     * @return the requested object
     */
    protected static <T> T get(Jedis conn, String key, Type type, Path... paths) {
        byte[][] args = new byte[1 + paths.length][];
        int i=0;
        args[i] = SafeEncoder.encode(key);
        for (Path p :paths) {
            args[++i] = SafeEncoder.encode(p.toString());
        }

        String rep;
        conn.getClient().sendCommand(Command.GET, args);
        rep = conn.getClient().getBulkReply();
        if (rep == null) {
            return null;
        }
        assertReplyNotError(rep);
        return gson.fromJson(rep, type);
    }

    protected static Boolean setnx(Jedis conn, String key, Object object, Path path) {

        List<byte[]> args = new ArrayList<>(4);

        args.add(SafeEncoder.encode(key));
        args.add(SafeEncoder.encode(path.toString()));
        args.add(SafeEncoder.encode(gson.toJson(object)));
        args.add(ExistenceModifier.NOT_EXISTS.getRaw());

        conn.getClient()
                .sendCommand(Command.SET, args.toArray(new byte[args.size()][]));
        String status = conn.getClient().getStatusCodeReply();
        assertReplyOK(status);
        return status != null;
    }

    protected static void arrAppend(Jedis conn, String key, Object object, Path path) {
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
                try {
                    conn.getClient()
                            .sendCommand(Command.SET, args.toArray(new byte[args.size()][]));
                    status = conn.getClient().getStatusCodeReply();
                } catch (JedisDataException e1) {
                    if (e1.getMessage().startsWith("ERR missing")) {
                        throw new KeyNotFoundException(e.getMessage());
                    }
                    throw new JedisDataException(e.getMessage());
                }
            } else {
                throw new JedisDataException(e.getMessage());
            }
        }
        assertReplyNotError(status);
    }

    protected static void strAppend(Jedis conn, String key, Object object, Path path) {

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
                try {
                    conn.getClient()
                            .sendCommand(Command.SET, args.toArray(new byte[args.size()][]));
                    status = conn.getClient().getStatusCodeReply();
                } catch (JedisDataException e1) {
                    if (e1.getMessage().startsWith("ERR missing")) {
                        throw new KeyNotFoundException(e.getMessage());
                    }
                    throw new JedisDataException(e.getMessage());
                }
            } else {
                throw new JedisDataException(e.getMessage());
            }
        }
        assertReplyOK(status);
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
