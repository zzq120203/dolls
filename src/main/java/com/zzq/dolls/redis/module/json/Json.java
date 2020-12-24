package com.zzq.dolls.redis.module.json;

import redis.clients.jedis.commands.ProtocolCommand;
import redis.clients.jedis.util.SafeEncoder;

import java.lang.reflect.Type;

public interface Json {

    enum ExistenceModifier implements ProtocolCommand {
        DEFAULT(""),
        NOT_EXISTS("NX"),
        MUST_EXIST("XX");
        private final byte[] raw;

        ExistenceModifier(String alt) {
            raw = SafeEncoder.encode(alt);
        }

        public byte[] getRaw() {
            return raw;
        }
    }

    Long del(final String key, final Path path);

    <T> T get(String key, Type type, Path... paths);

    <T> T get(String key, Path... paths);

    <T> T get(String key, Class<T> clazz, Path... paths);

    void set(String key, Object object, ExistenceModifier flag);

    void set(String key, Object object);

    void set(String key, Object object, Path path);

    void set(String key, Object object, ExistenceModifier flag, Path path);

    Boolean setnx(String key, Object object, Path path);

    void arrAppend(String key, Object object, Path path);

    void strAppend(String key, Object object, Path path);

    Class<?> type(String key, Path path);


}
