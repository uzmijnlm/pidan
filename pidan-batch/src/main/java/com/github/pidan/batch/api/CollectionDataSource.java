package com.github.pidan.batch.api;

import com.github.pidan.batch.environment.ExecutionEnvironment;
import com.github.pidan.core.Partition;
import com.github.pidan.core.TaskContext;
import com.github.pidan.core.tuple.Tuple2;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class CollectionDataSource<E> extends DataSet<E> {

    private final Collection<E> collection;
    private final int parallelism;

    public CollectionDataSource(ExecutionEnvironment env, Collection<E> collection, int parallelism) {
        super(env);
        this.collection = collection;
        this.parallelism = parallelism;
    }

    @Override
    public Partition[] getPartitions() {
        return slice(collection, parallelism);
    }

    private Partition[] slice(Collection<E> collection, int parallelism) {
        long length = collection.size();
        List<Tuple2<Integer, Integer>> indexRangeList = new ArrayList<>();
        for (int i = 0; i < parallelism; i++) {
            int start = (int) ((i * length) / parallelism);
            int end = (int) (((i + 1) * length) / parallelism);
            indexRangeList.add(Tuple2.of(start, end));
        }

        List<E> list = ImmutableList.copyOf(collection);
        ParallelCollectionPartition[] partitions = new ParallelCollectionPartition[indexRangeList.size()];
        for (int i = 0; i < partitions.length; i++) {
            Tuple2<Integer, Integer> indexRange = indexRangeList.get(i);
            partitions[i] = new ParallelCollectionPartition<>(i, list.subList(indexRange.f0, indexRange.f1));
        }
        return partitions;
    }

    @Override
    public Iterator<E> compute(Partition partition, TaskContext taskContext) {
        ParallelCollectionPartition<E> collectionPartition = (ParallelCollectionPartition<E>) partition;
        return collectionPartition.getCollection().iterator();
    }

    private static class ParallelCollectionPartition<ROW>
            extends Partition {
        private final Collection<ROW> collection;

        public ParallelCollectionPartition(int index, Collection<ROW> collection) {
            super(index);
            this.collection = collection;
        }

        public Collection<ROW> getCollection() {
            return collection;
        }
    }
}
