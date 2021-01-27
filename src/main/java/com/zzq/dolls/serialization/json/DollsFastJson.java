package com.zzq.dolls.serialization.json;

import com.alibaba.fastjson.JSON;

import java.lang.reflect.Type;

public class DollsFastJson {

    public static String toJson(Object object) {
        return JSON.toJSONString(object);
    }

    public static <T> T fromJson(String json, Class<T> clazz) {
        return JSON.parseObject(json, clazz);
    }

    public static <T> T fromJson(String json, Type type) {
        return JSON.parseObject(json, type);
    }

}
