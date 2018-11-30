package zzq.dolls.function;


@FunctionalInterface
public interface BiFunctionThrows<T, U, R, E extends Exception> {
    R apply(T t, U u) throws E;
}