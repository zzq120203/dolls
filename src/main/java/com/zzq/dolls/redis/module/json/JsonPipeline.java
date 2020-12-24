package com.zzq.dolls.redis.module.json;

import redis.clients.jedis.Response;

public interface JsonPipeline {
    Response<Long> del(final String key, final Path path);

    Response<String> get(String key, Path... paths);

    void set(String key, Object object, Json.ExistenceModifier flag);

    void set(String key, Object object);

    void set(String key, Object object, Path path);

    void set(String key, Object object, Json.ExistenceModifier flag, Path path);

    Response<Boolean> setnx(String key, Object object, Path path);

    void arrAppend(String key, Object object, Path path);

    void strAppend(String key, Object object, Path path);

}
