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
        return new Partition[]{new ParallelCollectionPartition<>(0, collection)};
    }

    @Override
    public Iterator<E> compute(Partition partition) {
        return collection.iterator();
    }

    private static class ParallelCollectionPartition<ROW>
            extends Partition
    {
        private final Collection<ROW> collection;

        public ParallelCollectionPartition(int index, Collection<ROW> collection)
        {
            super(index);
            this.collection = collection;
        }

        public Collection<ROW> getCollection()
        {
            return collection;
        }
    }
}
