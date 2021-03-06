package com.github.pidan.batch.api;

import com.github.pidan.batch.runtime.TaskContext;
import com.github.pidan.core.function.FlatMapFunction;
import com.github.pidan.core.function.MapFunction;
import com.github.pidan.core.partition.Partition;
import com.google.common.collect.Iterators;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class FlatMapPartitionDataSet<IN, OUT> extends DataSet<OUT> {

    private final FlatMapFunction<IN, OUT> flatMapFunction;

    private final DataSet<IN> parentDataSet;

    public FlatMapPartitionDataSet(DataSet<IN> parentDataSet, FlatMapFunction<IN, OUT> flatMapFunction) {
        super(parentDataSet.getExecutionEnvironment());
        this.flatMapFunction = flatMapFunction;
        this.parentDataSet = parentDataSet;
    }

    public FlatMapPartitionDataSet(DataSet<IN> parentDataSet, MapFunction<IN, OUT[]> mapFunction) {
        super(parentDataSet);
        this.flatMapFunction = (input, collector) -> {
            for (OUT value : mapFunction.map(input)) {
                collector.collect(value);
            }
        };
        this.parentDataSet = parentDataSet;
    }

    @Override
    public Partition[] getPartitions() {
        return parentDataSet.getPartitions();
    }

    @Override
    public Iterator<OUT> compute(Partition partition, TaskContext taskContext) {
        return Iterators.concat(Iterators.transform(parentDataSet.compute(partition, taskContext), row -> {
            List<OUT> list = new ArrayList<>();
            flatMapFunction.flatMap(row, list::add);
            return list.iterator();
        }));
    }

}
