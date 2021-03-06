package com.github.pidan.batch.api;

import com.github.pidan.batch.runtime.TaskContext;
import com.github.pidan.core.function.MapFunction;
import com.github.pidan.core.partition.Partition;
import com.google.common.collect.Iterators;

import java.util.Iterator;

public class MapPartitionDataSet<IN, OUT> extends DataSet<OUT> {

    private final MapFunction<IN, OUT> mapFunction;

    private final DataSet<IN> parentDataSet;

    public MapPartitionDataSet(DataSet<IN> parentDataSet, MapFunction<IN, OUT> mapFunction) {
        super(parentDataSet);
        this.parentDataSet = parentDataSet;
        this.mapFunction = mapFunction;
    }

    @Override
    public Partition[] getPartitions() {
        return parentDataSet.getPartitions();
    }

    @Override
    public Iterator<OUT> compute(Partition partition, TaskContext taskContext) {
        return Iterators.transform(parentDataSet.compute(partition, taskContext), mapFunction::map);
    }

}
