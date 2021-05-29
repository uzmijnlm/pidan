package com.github.pidan.batch.api;

import com.github.pidan.batch.environment.ExecutionEnvironment;
import com.github.pidan.core.Partition;
import com.github.pidan.core.function.FilterFunction;
import com.github.pidan.core.function.FlatMapFunction;
import com.github.pidan.core.function.MapFunction;

import java.util.Iterator;
import java.util.List;

public abstract class DataSet<ROW> {

    protected final ExecutionEnvironment env;

    protected DataSet(ExecutionEnvironment env) {
        this.env = env;
    }

    public ExecutionEnvironment getExecutionEnvironment() {
        return env;
    }

    public abstract Partition[] getPartitions();

    public abstract Iterator<ROW> compute(Partition partition);

    public <OUT> DataSet<OUT> map(MapFunction<ROW, OUT> mapFunction) {
        return new MapPartitionDataSet<>(this, mapFunction);
    }

    public <OUT> DataSet<OUT> flatMap(MapFunction<ROW, OUT[]> mapFunction) {
        return new FlatMapPartitionDataSet<>(this, mapFunction);
    }

    public <OUT> DataSet<OUT> flatMap(FlatMapFunction<ROW, OUT> flatMapFunction)
    {
        return new FlatMapPartitionDataSet<>(this, flatMapFunction);
    }

    public DataSet<ROW> filter(FilterFunction<ROW> filterFunction) {
        return new FilterPartitionDataSet<>(this, filterFunction);
    }

    public List<ROW> collect() {
        return null;
    }

}
