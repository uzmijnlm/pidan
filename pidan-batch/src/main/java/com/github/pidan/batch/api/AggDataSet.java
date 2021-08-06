package com.github.pidan.batch.api;

import com.github.pidan.core.TaskContext;
import com.github.pidan.core.function.KeySelector;
import com.github.pidan.core.function.ReduceFunction;
import com.github.pidan.core.partition.Partition;
import com.github.pidan.core.util.IteratorUtil;

import java.util.Iterator;

import static com.github.pidan.core.configuration.Constant.enableSortShuffle;

public class AggDataSet<KEY, ROW> extends DataSet<ROW> {
    private final DataSet<ROW> parentDataSet;
    private final KeySelector<ROW, KEY> keySelector;
    private final ReduceFunction<ROW> reduceFunction;

    public AggDataSet(DataSet<ROW> parentDataSet,
                      KeySelector<ROW, KEY> keySelector,
                      ReduceFunction<ROW> reduceFunction) {
        super(parentDataSet);
        this.parentDataSet = parentDataSet;
        this.keySelector = keySelector;
        this.reduceFunction = reduceFunction;
    }


    @Override
    public Partition[] getPartitions() {
        return parentDataSet.getPartitions();
    }

    @Override
    public Iterator<ROW> compute(Partition partition, TaskContext taskContext) {
        Iterator<ROW> iterator = parentDataSet.compute(partition, taskContext);
        if (enableSortShuffle) {
            return IteratorUtil.reduceSorted(iterator, keySelector, reduceFunction);
        } else {
            return IteratorUtil.reduce(iterator, keySelector, reduceFunction);
        }
    }

}
