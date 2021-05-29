package com.github.pidan.batch.environment;


import com.github.pidan.batch.api.CollectionDataSource;

import java.util.Arrays;
import java.util.Collection;

public class ExecutionEnvironment {

    public static ExecutionEnvironment getExecutionEnvironment() {
        return new LocalExecutionEnvironment();
    }

    @SafeVarargs
    public final <X> CollectionDataSource<X> fromElements(X... data) {
        return fromCollection(Arrays.asList(data));
    }

    public <X> CollectionDataSource<X> fromCollection(Collection<X> data) {
        return new CollectionDataSource<>(this, data);
    }
}
