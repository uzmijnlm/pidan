package com.github.pidan.batch.environment;


import com.github.pidan.batch.api.CollectionDataSource;
import com.github.pidan.batch.api.DataSet;
import com.github.pidan.core.function.Foreach;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

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

    <ROW> void runJob(DataSet<ROW> dataSet, Foreach<Iterator<ROW>> foreach);
}
