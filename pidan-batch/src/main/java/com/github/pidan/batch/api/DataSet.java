package com.github.pidan.batch.api;

import com.github.pidan.batch.environment.ExecutionEnvironment;
import com.github.pidan.core.Partition;
import com.github.pidan.core.function.*;
import com.google.common.collect.ImmutableList;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

    public <KEY> KeyedDataSet<KEY, ROW> groupBy(KeySelector<ROW, KEY> keySelector) {
        return new KeyedDataSet<>(this, keySelector);
    }

    public List<ROW> collect() {
        Partition[] partitions = getPartitions();
        ExecutorService executors = Executors.newFixedThreadPool(partitions.length);
        try {
            return Stream.of(partitions).parallel().map(partition -> CompletableFuture.supplyAsync(() -> {
                Iterator<ROW> iterator = compute(partition);
                return ImmutableList.copyOf(iterator);
            }, executors)).flatMap(x -> x.join().stream())
                    .collect(Collectors.toList());
        }
        finally {
            executors.shutdown();
        }
    }

    public void foreach(Foreach<ROW> foreach) {
        this.getExecutionEnvironment().runJob(this, (Iterator<ROW> iterator) -> {
            while (iterator.hasNext()) {
                foreach.apply(iterator.next());
            }
        });
    }

}
