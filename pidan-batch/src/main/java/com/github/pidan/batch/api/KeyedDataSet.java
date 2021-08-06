package com.github.pidan.batch.api;

import com.github.pidan.core.function.*;
import com.github.pidan.core.tuple.Tuple2;
import com.github.pidan.core.util.TypeUtil;


public class KeyedDataSet<KEY, ROW> {

    private final DataSet<ROW> parentDataSet;
    private final KeySelector<ROW, KEY> keySelector;
    private final Partitioner partitioner;

    public KeyedDataSet(DataSet<ROW> parentDataSet, KeySelector<ROW, KEY> keySelector, Partitioner partitioner) {
        this.parentDataSet = parentDataSet;
        this.keySelector = keySelector;
        this.partitioner = partitioner;
    }

    public KeyedDataSet(DataSet<ROW> parentDataSet, KeySelector<ROW, KEY> keySelector) {
        this(parentDataSet, keySelector, new HashPartitioner(parentDataSet.numPartitions()));
    }

    public DataSet<ROW> reduce(ReduceFunction<ROW> reduceFunction) {
        ShuffleMapOperator<KEY, ROW> shuffleMapOperator = new ShuffleMapOperator<>(parentDataSet, keySelector, partitioner);
        ShuffleOperator<KEY, ROW> shuffleOperator = new ShuffleOperator<>(shuffleMapOperator, keySelector, partitioner);
        return new AggDataSet<>(shuffleOperator, keySelector, reduceFunction);
    }

    public DataSet<Tuple2<KEY, Integer>> count() {
        KeySelector<Tuple2<KEY, Integer>, KEY> newKeySelector = value -> value.f0;
        TypeUtil.extractKeyType(keySelector);
        ShuffleMapOperator<KEY, Tuple2<KEY, Integer>> shuffleMapOperator = new ShuffleMapOperator<>(
                parentDataSet.map((MapFunction<ROW, Tuple2<KEY, Integer>>) input -> Tuple2.of(keySelector.getKey(input), 1)),
                newKeySelector,
                partitioner);
        ShuffleOperator<KEY, Tuple2<KEY, Integer>> shuffleOperator = new ShuffleOperator<>(shuffleMapOperator, newKeySelector, partitioner);
        ReduceFunction<Tuple2<KEY, Integer>> reduceFunction = (input1, input2) -> Tuple2.of(input1.f0, input1.f1 + input2.f1);
        return new AggDataSet<>(shuffleOperator, (KeySelector<Tuple2<KEY, Integer>, KEY>) value -> value.f0, reduceFunction);
    }
}
