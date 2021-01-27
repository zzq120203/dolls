package com.zzq.dolls.redis.module.json;

import com.zzq.dolls.redis.RedisMode;
import com.zzq.dolls.redis.RedisPool;
import com.zzq.dolls.redis.exception.KeyNotFoundException;
import com.zzq.dolls.redis.mini.JedisMini;
import com.zzq.dolls.serialization.json.DollsJson;
import redis.clients.jedis.commands.ProtocolCommand;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.util.SafeEncoder;

import java.util.ArrayList;
import java.util.List;

public class RedisJson extends JedisMini implements Json {

    public RedisJson(RedisPool pool, RedisMode mode) {
        super(pool, mode);
    }

    @Override
    public Long del(String key, Path path) {
        byte[][] args = new byte[2][];
        args[0] = SafeEncoder.encode(key);
        args[1] = SafeEncoder.encode(path.toString());
        Object o = sendCommand(Long.class, Command.DEL, args);
        return o == null ? 0 : Long.parseLong(o.toString());
    }

    @Override
    public <T> T get(String key, Path... paths) {
        return (T) get(key, Object.class, paths);
    }

    @Override
    public void set(String key, Object object, ExistenceModifier flag) {
        set(key, object, flag, Path.ROOT_PATH);
    }

    @Override
    public void set(String key, Object object) {
        set(key, object, ExistenceModifier.DEFAULT, Path.ROOT_PATH);
    }

    @Override
    public void set(String key, Object object, Path path) {
        set(key, object, ExistenceModifier.DEFAULT, path);
    }

    @Override
    public void set(String key, Object object, ExistenceModifier flag, Path path) {

        List<byte[]> args = new ArrayList<>(4);

        args.add(SafeEncoder.encode(key));
        args.add(SafeEncoder.encode(path.toString()));
        args.add(SafeEncoder.encode(DollsJson.toJson(object)));
        if (ExistenceModifier.DEFAULT != flag) {
            args.add(flag.getRaw());
        }

        Object status = sendCommand(String.class, Command.SET, args.toArray(new byte[args.size()][]));
        assertReplyOK(status);
    }

    @Override
    public Class<?> type(String key, Path path) {
        List<byte[]> args = new ArrayList<>(2);

        args.add(SafeEncoder.encode(key));
        args.add(SafeEncoder.encode(path.toString()));

        Object rep = sendCommand(String.class, Command.TYPE, args.toArray(new byte[args.size()][]));

        assertReplyNotError(rep);
        String type = rep == null ? "null" : rep.toString();
        switch (type) {
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
                throw new java.lang.RuntimeException(rep.toString());
        }
    }

    protected enum Command implements ProtocolCommand {
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
     * Helper to handle single optional path argument situations
     *
     * @param path a single optional path
     * @return the provided path or root if not
     */
    private static Path getSingleOptionalPath(Path... path) {
        // check for 0, 1 or more paths
        if (1 > path.length) {   // default to root
            return Path.ROOT_PATH;
        }
        if (1 == path.length) {  // take 1
            return path[0];
        }

        // throw out the baby with the water
        throw new RuntimeException("Only a single optional path is allowed");
    }

    private <T> Object sendCommand(Class<T> clazz, Command command, byte[]... args) {
        if (mode == RedisMode.CLUSTER) {
            Object result = pool.cluster(cluster -> cluster.sendCommand(args[0], command, args));
            if (clazz == String.class) {
                return result == null ? null : new String((byte[]) result);
            } else if (clazz == Integer.class || clazz == Long.class) {
                return result == null ? null : Long.parseLong(result.toString());
            } else {
                return result;
            }
        } else {
            return pool.jedis(jedis -> {
                jedis.getClient()
                        .sendCommand(command, args);
                if (clazz == String.class) {
                    return jedis.getClient().getBulkReply();
                } else if (clazz == Integer.class || clazz == Long.class) {
                    return jedis.getClient().getIntegerReply();
                } else if (clazz == List.class) {
                    return jedis.getClient().getMultiBulkReply();
                } else {
                    return jedis.getClient().getBinaryBulkReply();
                }
            });
        }
    }

    @Override
    public <T> T get(String key, Class<T> clazz, Path... paths) {

        List<byte[]> args = new ArrayList<>(2);
        args.add(SafeEncoder.encode(key));
        // 中文解码
        args.add(SafeEncoder.encode("NOESCAPE"));
        for (Path p : paths) {
            args.add(SafeEncoder.encode(p.toString()));
        }

        Object rep = sendCommand(byte[].class, Command.GET, args.toArray(new byte[args.size()][]));
        assertReplyNotError(rep);
        return rep == null ? null : DollsJson.fromJson(new String((byte[])rep), clazz);
    }

    @Override
    public Boolean setnx(String key, Object object, Path path) {

        List<byte[]> args = new ArrayList<>(4);

        args.add(SafeEncoder.encode(key));
        args.add(SafeEncoder.encode(path.toString()));
        args.add(SafeEncoder.encode(DollsJson.toJson(object)));
        args.add(ExistenceModifier.NOT_EXISTS.getRaw());

        Object status = sendCommand(String.class, Command.SET, args.toArray(new byte[args.size()][]));
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
                    args.add(SafeEncoder.encode(DollsJson.toJson(obj)))
            );
        } else {
            args.add(SafeEncoder.encode(DollsJson.toJson(object)));
        }

        Object status;
        try {
            status = sendCommand(Long.class, Command.ARRAPPEND, args.toArray(new byte[args.size()][]));
        } catch (JedisDataException e) {
            if (e.getMessage().startsWith("ERR key")) {
                // arr is not exist , set

                args.clear();
                args.add(SafeEncoder.encode(key));
                args.add(SafeEncoder.encode(path.toString()));
                if (object instanceof List) {
                    args.add(SafeEncoder.encode(DollsJson.toJson(object)));
                } else {
                    List<Object> objects = new ArrayList<>();
                    objects.add(object);
                    args.add(SafeEncoder.encode(DollsJson.toJson(objects)));
                }
                try {
                    status = sendCommand(String.class, Command.SET, args.toArray(new byte[args.size()][]));
                } catch (JedisDataException e1) {
                    if (e1.getMessage().startsWith("ERR missing")
                            || e1.getMessage().startsWith("ERR new")) {
                        throw new KeyNotFoundException(e1.getMessage());
                    }
                    throw new JedisDataException(e1.getMessage());
                }
            } else if (e.getMessage().startsWith("WRONGTYPE Operation")) {
                throw new KeyNotFoundException(e.getMessage());
            } else {
                throw new JedisDataException(e.getMessage());
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
                    args.add(SafeEncoder.encode(DollsJson.toJson(obj)))
            );
        } else {
            args.add(SafeEncoder.encode(DollsJson.toJson(object)));
        }

        Object status;
        try {
            status = sendCommand(Long.class, Command.STRAPPEND, args.toArray(new byte[args.size()][]));
        } catch (JedisDataException e) {
            if (e.getMessage().startsWith("ERR key")) {
                // str is not exist , set
                args.clear();
                args.add(SafeEncoder.encode(key));
                args.add(SafeEncoder.encode(path.toString()));
                args.add(SafeEncoder.encode(DollsJson.toJson(object)));
                try {
                    status = sendCommand(String.class, Command.SET, args.toArray(new byte[args.size()][]));
                } catch (JedisDataException e1) {
                    if (e1.getMessage().startsWith("ERR missing")
                            || e1.getMessage().startsWith("ERR new")) {
                        throw new KeyNotFoundException(e1.getMessage());
                    }
                    throw new JedisDataException(e1.getMessage());
                }
            } else if (e.getMessage().startsWith("WRONGTYPE Operation")) {
                throw new KeyNotFoundException(e.getMessage());
            } else {
                throw new JedisDataException(e.getMessage());
            }
        }
        assertReplyOK(status);
    }

    private void assertReplyOK(final Object str) {
        if (str instanceof String && !str.toString().equals("OK"))
            throw new RuntimeException(new String((byte[]) str));
    }

    private void assertReplyNotError(final Object str) {
        if (str instanceof String && str.toString().startsWith("-ERR"))
            throw new RuntimeException(str.toString().substring(5));
    }
}
