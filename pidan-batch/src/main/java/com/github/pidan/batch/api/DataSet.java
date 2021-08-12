package com.github.pidan.batch.api;

import com.github.pidan.batch.environment.ExecutionEnvironment;
import com.github.pidan.batch.runtime.TaskContext;
import com.github.pidan.core.JoinType;
import com.github.pidan.core.function.*;
import com.github.pidan.core.partition.Partition;
import com.google.common.collect.ImmutableList;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public abstract class DataSet<ROW> implements Serializable {

    protected final transient ExecutionEnvironment env;
    protected final DataSet<?>[] dataSets;

    protected DataSet(ExecutionEnvironment env) {
        this.env = env;
        this.dataSets = new DataSet<?>[0];
    }

    protected DataSet(DataSet<?>... dataSets) {
        this.env = dataSets[0].getExecutionEnvironment();
        this.dataSets = dataSets;
    }

    public ExecutionEnvironment getExecutionEnvironment() {
        return env;
    }

    public abstract Partition[] getPartitions();

    public List<DataSet<?>> getDependencies() {
        return Arrays.asList(dataSets);
    }

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
        return this.getExecutionEnvironment().runJob(this, ImmutableList::copyOf)
                .stream()
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

    public <I2> JoinedDataSet<ROW, I2> join(DataSet<I2> dataSet2) {
        return new JoinedDataSet<>(this, dataSet2);
    }

    public <I2> JoinedDataSet<ROW, I2> join(DataSet<I2> dataSet2, JoinType joinType) {
        return new JoinedDataSet<>(this, dataSet2, joinType);
    }
}
