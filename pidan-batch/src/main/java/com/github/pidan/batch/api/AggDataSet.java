package com.github.pidan.batch.api;

import com.github.pidan.core.Partition;
import com.github.pidan.core.function.KeySelector;
import com.github.pidan.core.function.ReduceFunction;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class AggDataSet<KEY, ROW> extends DataSet<ROW> {
    private final ShuffleOperator<KEY, ROW> shuffleOperator;
    private final KeySelector<ROW, KEY> groupKeySelector;
    private final ReduceFunction<ROW> reduceFunction;

    public AggDataSet(ShuffleOperator<KEY, ROW> shuffleOperator,
                      KeySelector<ROW, KEY> groupKeySelector,
                      ReduceFunction<ROW> reduceFunction) {
        super(shuffleOperator.getExecutionEnvironment());
        this.shuffleOperator = shuffleOperator;
        this.groupKeySelector = groupKeySelector;
        this.reduceFunction = reduceFunction;
    }


    @Override
    public Partition[] getPartitions() {
        return shuffleOperator.getPartitions();
    }

    @Override
    public Iterator<ROW> compute(Partition partition) {
        Iterator<ROW> iterator = shuffleOperator.compute(partition);
        List<ROW> records = new ArrayList<>();
        while (iterator.hasNext()) {
            ROW record = iterator.next();
            records.add(record);
        }
        Map<KEY, List<ROW>> groupBy = records.stream().collect(Collectors.groupingBy(groupKeySelector::getKey));
        List<ROW> out = groupBy.values().stream().map(x -> x.stream().reduce(reduceFunction::reduce).get()).collect(Collectors.toList());
        return out.iterator();
    }

    @Override
    public DataSet<?> getParent() {
        return shuffleOperator;
    }
}
