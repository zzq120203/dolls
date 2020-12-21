package com.zzq.dolls.redis.module.json;

import redis.clients.jedis.commands.ProtocolCommand;
import redis.clients.jedis.util.SafeEncoder;

import java.lang.reflect.Type;

public interface Json {

    /**
     * Existential modifier for the set command, by default we don't care
     */
    public enum ExistenceModifier implements ProtocolCommand {
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

    public Long del(final String key, final Path path);

    public <T> T get(String key, Type type, Path... paths);

    public Long del(String key);

    public <T> T get(String key, Path... paths);

    /**
     * Gets an object
     * @param key the key name
     * @param clazz
     * @param paths optional one ore more paths in the object
     * @return the requested object
     */
    public <T> T get(String key, Class<T> clazz, Path... paths);

    /**
     * Sets an object at the root path
     * @param key the key name
     * @param object the Java object to store
     * @param flag an existential modifier
     */
    public void set(String key, Object object, ExistenceModifier flag);

    /**
     * Sets an object in the root path
     * @param key the key name
     * @param object the Java object to store
     */
    public void set(String key, Object object);

    /**
     * Sets an object without caring about target path existing
     * @param key the key name
     * @param object the Java object to store
     * @param path in the object
     */
    public void set(String key, Object object, Path path);

    public void set(String key, Object object, ExistenceModifier flag, Path path);

    public Boolean setnx(String key, Object object, Path path);

    public void arrAppend(String key, Object object, Path path);


    public void strAppend(String key, Object object, Path path);

    public Class<?> type(String key, Path path);


}
