package zzq.dolls.function;


@FunctionalInterface
public interface FunctionThrows<T, R, E extends Exception> {
    R apply(T t) throws E;
}