package com.github.pidan.batch.api;

import com.github.pidan.core.Partition;
import com.github.pidan.core.TaskContext;
import com.github.pidan.core.function.KeySelector;
import com.github.pidan.core.util.IteratorUtil;

import java.util.Iterator;
import java.util.stream.IntStream;

import static com.github.pidan.core.configuration.Constant.enableSortShuffle;

public class ShuffleOperator<KEY, ROW> extends DataSet<ROW> {
    private final ShuffleMapOperator<KEY, ROW> shuffleMapOperator;
    private final KeySelector<ROW, KEY> keySelector;

    public ShuffleOperator(ShuffleMapOperator<KEY, ROW> shuffleMapOperator, KeySelector<ROW, KEY> keySelector) {
        super(shuffleMapOperator);
        this.shuffleMapOperator = shuffleMapOperator;
        this.keySelector = keySelector;
    }


    @Override
    public Partition[] getPartitions() {
        return IntStream.range(0, shuffleMapOperator.getPartitioner().numPartitions())
                .mapToObj(Partition::new).toArray(Partition[]::new);
    }

    @Override
    public Iterator<ROW> compute(Partition partition, TaskContext taskContext) {
        int index = partition.getIndex();
        int[] dependencies = taskContext.getDependStages();
        ShuffleReader shuffleReader = new ShuffleReader(index, dependencies[0]);
        Iterator<ROW> iterator = (Iterator<ROW>) shuffleReader.read();
        if (enableSortShuffle) {
            return IteratorUtil.sortIterator(iterator, keySelector);
        } else {
            return iterator;
        }
    }

    @Override
    public int numPartitions() {
        return shuffleMapOperator.getPartitioner().numPartitions();
    }

    @Override
    public DataSet<?> getParent() {
        return shuffleMapOperator;
    }
}
