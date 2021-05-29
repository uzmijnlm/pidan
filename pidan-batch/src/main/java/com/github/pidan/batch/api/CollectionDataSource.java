package com.github.pidan.batch.api;

import com.github.pidan.batch.environment.ExecutionEnvironment;
import com.github.pidan.core.Partition;

import java.util.Collection;
import java.util.Iterator;

public class CollectionDataSource<E> extends DataSet<E> {

    private final Collection<E> collection;

    public CollectionDataSource(ExecutionEnvironment env, Collection<E> collection) {
        super(env);
        this.collection = collection;
    }

    @Override
    public Partition[] getPartitions() {
        return new Partition[0];
    }

    @Override
    public Iterator<E> compute(Partition partition) {
        return collection.iterator();
    }
}
