package com.github.pidan.batch.environment;


import com.github.pidan.batch.api.CollectionDataSource;
import com.github.pidan.batch.api.DataSet;
import com.github.pidan.batch.api.TextFileDataSource;
import com.github.pidan.core.function.MapFunction;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public interface ExecutionEnvironment {

    static ExecutionEnvironment getLocalExecutionEnvironment() {
        return new LocalExecutionEnvironment();
    }

    static ExecutionEnvironment getPseudoRemoteExecutionEnvironment() {
        return new PseudoRemoteExecutionEnvironment();
    }

    default <X> CollectionDataSource<X> fromElements(X... data) {
        return fromCollection(Arrays.asList(data), 1);
    }

    default <X> CollectionDataSource<X> fromCollection(Collection<X> data) {
        return new CollectionDataSource<>(this, data, 1);
    }

    default <X> CollectionDataSource<X> fromCollection(Collection<X> data, int parallelism) {
        return new CollectionDataSource<>(this, data, parallelism);
    }

    default TextFileDataSource textFile(String path) {
        return new TextFileDataSource(this, path);
    }

    <ROW, OUT> List<OUT> runJob(DataSet<ROW> dataSet, MapFunction<Iterator<ROW>, OUT> foreach);

    default void stop() {

    }

    default void stopForcibly() {

    }

}
