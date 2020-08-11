package com.zzq.dolls.function;

public interface ConsumerThrows<T, E extends Exception> {
    void apply(T t) throws E;

}
