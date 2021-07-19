package com.github.pidan.batch.api;

import com.github.pidan.batch.environment.ExecutionEnvironment;
import com.github.pidan.core.Partition;
import com.github.pidan.core.TaskContext;
import com.github.pidan.core.function.*;
import com.google.common.collect.ImmutableList;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public abstract class DataSet<ROW> {

    protected final ExecutionEnvironment env;

    protected DataSet(ExecutionEnvironment env) {
        this.env = env;
    }

    public ExecutionEnvironment getExecutionEnvironment() {
        return env;
    }

    public abstract Partition[] getPartitions();

    public int numPartitions() {
        return getPartitions().length;
    }

    public abstract Iterator<ROW> compute(Partition partition, TaskContext taskContext);

    public <OUT> DataSet<OUT> map(MapFunction<ROW, OUT> mapFunction) {
        return new MapPartitionDataSet<>(this, mapFunction);
    }

    public <OUT> DataSet<OUT> flatMap(MapFunction<ROW, OUT[]> mapFunction) {
        return new FlatMapPartitionDataSet<>(this, mapFunction);
    }

    public <OUT> DataSet<OUT> flatMap(FlatMapFunction<ROW, OUT> flatMapFunction) {
        return new FlatMapPartitionDataSet<>(this, flatMapFunction);
    }

    public DataSet<ROW> filter(FilterFunction<ROW> filterFunction) {
        return new FilterPartitionDataSet<>(this, filterFunction);
    }

    public <KEY> KeyedDataSet<KEY, ROW> groupBy(KeySelector<ROW, KEY> keySelector) {
        return new KeyedDataSet<>(this, keySelector);
    }

    public List<ROW> collect() {
        return this.getExecutionEnvironment().runJob(this, ImmutableList::copyOf).stream()
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    public void foreach(Foreach<ROW> foreach) {
        this.getExecutionEnvironment().runJob(this, (Iterator<ROW> iterator) -> {
            while (iterator.hasNext()) {
                foreach.apply(iterator.next());
            }
            return null;
        });
    }

    public DataSet<?> getParent() {
        return null;
    }
}
