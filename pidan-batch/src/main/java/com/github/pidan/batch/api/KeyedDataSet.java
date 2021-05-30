package com.github.pidan.batch.api;

import com.github.pidan.core.Partition;
import com.github.pidan.core.function.KeySelector;
import com.github.pidan.core.function.ReduceFunction;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class KeyedDataSet<KEY, ROW> {

    private final DataSet<ROW> parentDataSet;
    private final KeySelector<ROW, KEY> keySelector;

    public KeyedDataSet(DataSet<ROW> parentDataSet, KeySelector<ROW, KEY> keySelector) {
        this.parentDataSet = parentDataSet;
        this.keySelector = keySelector;
    }

    public DataSet<ROW> reduce(ReduceFunction<ROW> reduceFunction) {
        Partition[] partitions = parentDataSet.getPartitions();

        List<ROW> list = new ArrayList<>();
        for (Partition split : partitions) {
            Iterator<ROW> iterator = parentDataSet.compute(split);
            list.addAll(ImmutableList.copyOf(iterator));
        }

        Map<KEY, List<ROW>> groupBy = list.stream().collect(Collectors.groupingBy(keySelector::getKey));
        List<ROW> out = groupBy.values().stream().map(x -> x.stream().reduce(reduceFunction::reduce).get()).collect(Collectors.toList());
        return parentDataSet.getExecutionEnvironment().fromCollection(out);
    }

}
