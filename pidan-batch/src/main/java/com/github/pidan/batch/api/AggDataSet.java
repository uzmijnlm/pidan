package com.github.pidan.batch.api;

import com.github.pidan.core.Partition;
import com.github.pidan.core.TaskContext;
import com.github.pidan.core.function.KeySelector;
import com.github.pidan.core.function.MapFunction;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class AggDataSet<KEY, ROW> extends DataSet<ROW> {
    private final ShuffleOperator<KEY, ROW> shuffleOperator;
    private final KeySelector<ROW, KEY> groupKeySelector;
    private final MapFunction<Iterator<ROW>, ROW> aggMapFunction;

    public AggDataSet(ShuffleOperator<KEY, ROW> shuffleOperator,
                      KeySelector<ROW, KEY> groupKeySelector,
                      MapFunction<Iterator<ROW>, ROW> aggMapFunction) {
        super(shuffleOperator);
        this.shuffleOperator = shuffleOperator;
        this.groupKeySelector = groupKeySelector;
        this.aggMapFunction = aggMapFunction;
    }


    @Override
    public Partition[] getPartitions() {
        return shuffleOperator.getPartitions();
    }

    @Override
    public Iterator<ROW> compute(Partition partition, TaskContext taskContext) {
        Iterator<ROW> iterator = shuffleOperator.compute(partition, taskContext);
        List<ROW> records = new ArrayList<>();
        while (iterator.hasNext()) {
            ROW record = iterator.next();
            records.add(record);
        }
        Map<KEY, List<ROW>> groupBy = records.stream().collect(Collectors.groupingBy(groupKeySelector::getKey));
        Iterator<Iterator<ROW>> iterators = groupBy.values().stream().map(List::iterator).iterator();

        return new Iterator<ROW>() {
            @Override
            public boolean hasNext() {
                return iterators.hasNext();
            }

            @Override
            public ROW next() {
                return aggMapFunction.map(iterators.next());
            }
        };
    }

    @Override
    public DataSet<?> getParent() {
        return shuffleOperator;
    }
}
