package com.github.pidan.batch.api;

import com.github.pidan.core.TaskContext;
import com.github.pidan.core.function.KeySelector;
import com.github.pidan.core.function.Partitioner;
import com.github.pidan.core.partition.Partition;
import com.github.pidan.core.partition.ShuffledPartition;
import com.github.pidan.core.util.IteratorUtil;

import java.util.Iterator;
import java.util.stream.IntStream;

import static com.github.pidan.core.configuration.Constant.enableSortShuffle;

public class ShuffleOperator<KEY, ROW> extends DataSet<ROW> {
    private final ShuffleMapOperator<KEY, ROW> shuffleMapOperator;
    private final KeySelector<ROW, KEY> keySelector;
    private final Partitioner partitioner;

    public ShuffleOperator(ShuffleMapOperator<KEY, ROW> shuffleMapOperator,
                           KeySelector<ROW, KEY> keySelector,
                           Partitioner partitioner) {
        super(shuffleMapOperator);
        this.shuffleMapOperator = shuffleMapOperator;
        this.keySelector = keySelector;
        this.partitioner = partitioner;
    }

    // Shuffle后的第一个算子需要根据Shuffle过程中的Partitioner中设置的分区数来构造相同数目的ShuffledPartition
    @Override
    public Partition[] getPartitions() {
        return IntStream.range(0, partitioner.numPartitions())
                .mapToObj(ShuffledPartition::new).toArray(Partition[]::new);
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

    // Shuffle后的第一个算子的分区数等于Shuffle过程中的Partitioner中设置的分区数
    @Override
    public int numPartitions() {
        return partitioner.numPartitions();
    }

}
