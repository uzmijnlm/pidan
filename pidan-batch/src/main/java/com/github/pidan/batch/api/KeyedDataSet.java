package com.github.pidan.batch.api;

import com.github.pidan.core.Partition;
import com.github.pidan.core.function.HashPartitioner;
import com.github.pidan.core.function.KeySelector;
import com.github.pidan.core.function.Partitioner;
import com.github.pidan.core.function.ReduceFunction;
import com.github.pidan.core.tuple.Tuple2;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class KeyedDataSet<KEY, ROW> {

    private final DataSet<ROW> parentDataSet;
    private final KeySelector<ROW, KEY> groupKeySelector;
    private KeySelector<ROW, KEY> partitionKeySelector;
    private final Partitioner<KEY> partitioner;

    public KeyedDataSet(DataSet<ROW> parentDataSet, KeySelector<ROW, KEY> groupKeySelector,
                        KeySelector<ROW, KEY> partitionKeySelector, Partitioner<KEY> partitioner) {
        this.parentDataSet = parentDataSet;
        this.groupKeySelector = groupKeySelector;
        this.partitionKeySelector = partitionKeySelector;
        this.partitioner = partitioner;
    }

    public KeyedDataSet(DataSet<ROW> parentDataSet, KeySelector<ROW, KEY> keySelector, Partitioner<KEY> partitioner) {
        this(parentDataSet, keySelector, keySelector, partitioner);
    }

    public KeyedDataSet(DataSet<ROW> parentDataSet, KeySelector<ROW, KEY> keySelector) {
        this(parentDataSet, keySelector, new HashPartitioner<>(parentDataSet.numPartitions()));
    }

    public KeyedDataSet<KEY, ROW> partitionBy(KeySelector<ROW, KEY> keySelector) {
        this.partitionKeySelector = keySelector;
        return this;
    }

    public DataSet<ROW> reduce(ReduceFunction<ROW> reduceFunction) {
        ShuffleMapOperator<KEY, ROW> shuffleMapOperator = new ShuffleMapOperator<>(parentDataSet, partitionKeySelector, partitioner);
        ShuffleOperator<KEY, ROW> shuffleOperator = new ShuffleOperator<>(shuffleMapOperator);
        return new AggDataSet<>(shuffleOperator, groupKeySelector, reduceFunction);
    }

    public DataSet<Tuple2<KEY, Integer>> count() {
        Partition[] partitions = parentDataSet.getPartitions();

        List<ROW> list = new ArrayList<>();
        for (Partition split : partitions) {
            Iterator<ROW> iterator = parentDataSet.compute(split);
            list.addAll(ImmutableList.copyOf(iterator));
        }

        Map<KEY, List<ROW>> groupBy = list.stream().collect(Collectors.groupingBy(groupKeySelector::getKey));
        List<Tuple2<KEY, Integer>> collect = groupBy.entrySet().stream()
                .map(entry -> Tuple2.of(entry.getKey(), entry.getValue().size()))
                .collect(Collectors.toList());
        return parentDataSet.getExecutionEnvironment().fromCollection(collect);
    }
}
