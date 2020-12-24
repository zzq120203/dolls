package com.zzq.dolls.redis.module.json;

import redis.clients.jedis.*;
import redis.clients.jedis.util.SafeEncoder;

import java.io.Closeable;
import java.io.IOException;

public class RedisJsonPipeline extends PipelineBase implements JsonPipeline, Closeable {

    private Client client = null;


    public void setClient(Client client) {
        this.client = client;
    }

    @Override
    protected Client getClient(String key) {
        return client;
    }

    @Override
    protected Client getClient(byte[] key) {
        return client;
    }

    @Override
    public Response<Long> del(String key, Path path) {
        byte[][] args = new byte[2][];
        args[0] = SafeEncoder.encode(key);
        args[1] = SafeEncoder.encode(path.toString());
        getClient(key).sendCommand(RedisJson.Command.DEL, args);
        return getResponse(BuilderFactory.LONG);
    }

    @Override
    public Response<String> get(String key, Path... paths) {
        byte[][] args = new byte[1 + paths.length][];
        int i=0;
        args[i] = SafeEncoder.encode(key);
        for (Path p :paths) {
            args[++i] = SafeEncoder.encode(p.toString());
        }
        getClient(key).sendCommand(RedisJson.Command.GET, args);
        return getResponse(BuilderFactory.STRING);
    }


    @Override
    public void set(String key, Object object, Json.ExistenceModifier flag) {

    }

    @Override
    public void set(String key, Object object) {

    }

    @Override
    public void set(String key, Object object, Path path) {

    }

    @Override
    public void set(String key, Object object, Json.ExistenceModifier flag, Path path) {

    }

    @Override
    public Response<Boolean> setnx(String key, Object object, Path path) {
        return null;
    }

    @Override
    public void arrAppend(String key, Object object, Path path) {

    }

    @Override
    public void strAppend(String key, Object object, Path path) {

    }

    @Override
    public void close() throws IOException {

    }

}
