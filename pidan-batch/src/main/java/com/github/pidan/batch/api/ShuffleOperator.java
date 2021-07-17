package com.github.pidan.batch.api;

import com.github.pidan.batch.shuffle.ShuffleManager;
import com.github.pidan.core.Partition;

import java.util.Iterator;
import java.util.stream.IntStream;

public class ShuffleOperator<KEY, ROW> extends DataSet<ROW> {
    private final ShuffleMapOperator<KEY, ROW> shuffleMapOperator;

    public ShuffleOperator(ShuffleMapOperator<KEY, ROW> shuffleMapOperator) {
        super(shuffleMapOperator.getExecutionEnvironment());
        this.shuffleMapOperator = shuffleMapOperator;
    }


    @Override
    public Partition[] getPartitions() {
        return IntStream.range(0, shuffleMapOperator.getPartitioner().numPartitions())
                .mapToObj(Partition::new).toArray(Partition[]::new);
    }

    @Override
    public Iterator<ROW> compute(Partition partition) {
        int index = partition.getIndex();
        return (Iterator<ROW>) ShuffleManager.read(index);
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
