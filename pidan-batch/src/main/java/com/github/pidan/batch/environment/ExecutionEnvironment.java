package com.github.pidan.batch.environment;


import com.github.pidan.batch.api.CollectionDataSource;
import com.github.pidan.batch.api.DataSet;
import com.github.pidan.batch.api.TextFileDataSource;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

public interface ExecutionEnvironment {

    static ExecutionEnvironment getExecutionEnvironment() {
        return new LocalExecutionEnvironment();
    }

    default <X> CollectionDataSource<X> fromElements(X... data) {
        return fromCollection(Arrays.asList(data), 1);
    }

    default  <X> CollectionDataSource<X> fromCollection(Collection<X> data) {
        return new CollectionDataSource<>(this, data, 1);
    }

    default  <X> CollectionDataSource<X> fromCollection(Collection<X> data, int parallelism) {
        return new CollectionDataSource<>(this, data, parallelism);
    }

    <ROW, OUT> List<OUT> runJob(DataSet<ROW> dataSet, Function<Iterator<ROW>, OUT> foreach);

    default TextFileDataSource textFile(String path) {
        return new TextFileDataSource(this, path);
    }
}
