package com.zzq.dolls.serialization.json;

@FunctionalInterface
public interface ToJsonMapper {

    String map(Object obj);
}
