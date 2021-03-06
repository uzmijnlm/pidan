package com.github.pidan.batch.api;

import com.github.pidan.batch.runtime.TaskContext;
import com.github.pidan.core.function.FilterFunction;
import com.github.pidan.core.partition.Partition;
import com.google.common.collect.Iterators;

import java.util.Iterator;

public class FilterPartitionDataSet<ROW> extends DataSet<ROW> {

    private final FilterFunction<ROW> filterFunction;

    private final DataSet<ROW> parentDataSet;

    public FilterPartitionDataSet(DataSet<ROW> parentDataSet, FilterFunction<ROW> filterFunction) {
        super(parentDataSet);
        this.filterFunction = filterFunction;
        this.parentDataSet = parentDataSet;
    }

    @Override
    public Partition[] getPartitions() {
        return parentDataSet.getPartitions();
    }

    @Override
    public Iterator<ROW> compute(Partition partition, TaskContext taskContext) {
        return Iterators.filter(parentDataSet.compute(partition, taskContext), filterFunction::filter);
    }

}
