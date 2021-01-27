package com.zzq.dolls.serialization.json;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;

public class DollsJackson {

    private static ObjectMapper objectMapper = null;

    private static ObjectMapper defaultObjectMapper;

    public static void configure(ObjectMapper staticObjectMapper) {
        objectMapper = staticObjectMapper;
    }

    public static ObjectMapper getObjectMapper() {
        return (objectMapper == null ? getDefaultObjectMapper(): objectMapper);
    }

    private static ObjectMapper getDefaultObjectMapper() {
        if (defaultObjectMapper == null) {
            defaultObjectMapper = defaultObjectMapper();
        }
        return defaultObjectMapper;
    }

    public static ObjectMapper defaultObjectMapper() {
        try {
            return new ObjectMapper()
                    .registerModule((Module) Class.forName("com.fasterxml.jackson.module.kotlin.KotlinModule").getConstructor().newInstance());
        } catch (ReflectiveOperationException e) {
            return new ObjectMapper();
        }
    }

    public static String toJson(Object object) {
        try {
            return (objectMapper == null ? getDefaultObjectMapper() : objectMapper).writeValueAsString(object);
        } catch (Exception e) {
            return null;
        }
    }

    public static <T> T fromJson(String json, Class<T> clazz) {
        try {
            return (objectMapper == null ? getDefaultObjectMapper() : objectMapper).readValue(json, clazz);
        } catch (Exception e) {
            return null;
        }
    }

}
