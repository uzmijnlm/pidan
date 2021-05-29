package com.github.pidan.core.io;

public interface Collector<T>
{

    void collect(T record);

    default void close() {}
}