package com.github.pidan.batch.api;

import com.github.pidan.core.Partition;
import com.github.pidan.core.function.MapFunction;
import com.google.common.collect.Iterators;

import java.util.Iterator;

public class MapPartitionDataSet<IN, OUT> extends DataSet<OUT> {

    private final MapFunction<IN, OUT> mapFunction;

    private final DataSet<IN> parentDataSet;

    public MapPartitionDataSet(DataSet<IN> parentDataSet, MapFunction<IN, OUT> mapFunction) {
        super(parentDataSet.getExecutionEnvironment());
        this.parentDataSet = parentDataSet;
        this.mapFunction = mapFunction;
    }

    @Override
    public Partition[] getPartitions() {
        return new Partition[0];
    }

    @Override
    public Iterator<OUT> compute(Partition partition) {
        return Iterators.transform(parentDataSet.compute(partition), mapFunction::map);
    }
}
