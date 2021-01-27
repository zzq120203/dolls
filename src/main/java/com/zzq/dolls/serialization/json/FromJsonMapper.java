package com.zzq.dolls.serialization.json;

@FunctionalInterface
public interface FromJsonMapper {
    <T> T map(String json, Class<T> tClass);
}
