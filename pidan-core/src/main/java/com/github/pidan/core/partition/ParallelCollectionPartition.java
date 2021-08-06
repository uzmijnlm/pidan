package com.github.pidan.core.partition;

import java.util.Collection;

public class ParallelCollectionPartition<ROW> implements Partition {
    private final int index;
    private final Collection<ROW> collection;

    public ParallelCollectionPartition(int index, Collection<ROW> collection) {
        this.index = index;
        this.collection = collection;
    }

    public Collection<ROW> getCollection() {
        return collection;
    }

    @Override
    public int getIndex() {
        return index;
    }

    @Override
    public int hashCode() {
        return index;
    }
}
