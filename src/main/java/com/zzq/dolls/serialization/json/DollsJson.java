package com.zzq.dolls.serialization.json;

import java.lang.reflect.Type;

public class DollsJson {

    private static FromJsonMapper fromJsonMapper = DollsFastJson::fromJson;

    private static ToJsonMapper toJsonMapper = DollsFastJson::toJson;

    private static FromJsonTypeMapper fromJsonTypeMapper = DollsFastJson::fromJson;

    public static void setFromJsonMapper(FromJsonMapper fromJsonMapper) {
        DollsJson.fromJsonMapper = fromJsonMapper;
    }

    public static void setToJsonMapper(ToJsonMapper toJsonMapper) {
        DollsJson.toJsonMapper = toJsonMapper;
    }

    public static void setFromJsonTypeMapper(FromJsonTypeMapper fromJsonTypeMapper) {
        DollsJson.fromJsonTypeMapper = fromJsonTypeMapper;
    }

    public static String toJson(Object obj) {
        return toJsonMapper.map(obj);
    }

    public static <T> T fromJson(String json, Class<T> targetClass) {
        return fromJsonMapper.map(json, targetClass);
    }

    public static <T> T fromJson(String json, Type type) {
        return fromJsonTypeMapper.map(json, type);
    }

}
