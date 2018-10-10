package cn.ac.iie.dolls.mq;

public interface DataFactory<T> {
    T newInstance();
}