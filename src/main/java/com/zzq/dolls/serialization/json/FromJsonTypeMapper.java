package com.zzq.dolls.serialization.json;

import java.lang.reflect.Type;

@FunctionalInterface
public interface FromJsonTypeMapper {
    <T> T map(String json, Type type);
}
