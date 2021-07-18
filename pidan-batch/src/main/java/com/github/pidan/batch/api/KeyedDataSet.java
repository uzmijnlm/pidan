package com.github.pidan.batch.api;

import com.github.pidan.core.function.*;
import com.github.pidan.core.tuple.Tuple2;

import java.util.Iterator;

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
        MapFunction<Iterator<ROW>, ROW> aggMapFunction = iterator -> {
            ROW lastVal = null;
            while (iterator.hasNext()) {
                ROW curVal = iterator.next();
                if (lastVal != null) {
                    lastVal = reduceFunction.reduce(lastVal, curVal);
                } else {
                    lastVal = curVal;
                }
            }
            return lastVal;
        };
        return new AggDataSet<>(shuffleOperator, groupKeySelector, aggMapFunction);
    }

    public DataSet<Tuple2<KEY, Integer>> count() {
        ShuffleMapOperator<KEY, Tuple2<KEY, Integer>> shuffleMapOperator = new ShuffleMapOperator<>(
                parentDataSet.map((MapFunction<ROW, Tuple2<KEY, Integer>>) input -> Tuple2.of(groupKeySelector.getKey(input), 1)),
                (KeySelector<Tuple2<KEY, Integer>, KEY>) value -> value.f0,
                partitioner);
        ShuffleOperator<KEY, Tuple2<KEY, Integer>> shuffleOperator = new ShuffleOperator<>(shuffleMapOperator);
        ReduceFunction<Tuple2<KEY, Integer>> reduceFunction = (input1, input2) -> Tuple2.of(input1.f0, input1.f1 + input2.f1);
        MapFunction<Iterator<Tuple2<KEY, Integer>>, Tuple2<KEY, Integer>> aggMapFunction = iterator -> {
            Tuple2<KEY, Integer> lastVal = null;
            while (iterator.hasNext()) {
                Tuple2<KEY, Integer> curVal = iterator.next();
                if (lastVal != null) {
                    lastVal = reduceFunction.reduce(lastVal, curVal);
                } else {
                    lastVal = curVal;
                }
            }
            return lastVal;
        };
        return new AggDataSet<>(shuffleOperator, (KeySelector<Tuple2<KEY, Integer>, KEY>) value -> value.f0, aggMapFunction);
    }
}
