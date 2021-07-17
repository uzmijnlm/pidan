package com.github.pidan.batch.api;

import com.github.pidan.core.Partition;
import com.github.pidan.core.function.KeySelector;
import com.github.pidan.core.function.Partitioner;
import com.sun.xml.internal.xsom.impl.scd.Iterators;

import java.io.IOException;
import java.util.Iterator;

public class ShuffleMapOperator<KEY, ROW> extends DataSet<ROW> {
    private final DataSet<ROW> parentDataSet;
    private final KeySelector<ROW, KEY> partitionKeySelector;
    private final Partitioner<KEY> partitioner;

    public ShuffleMapOperator(DataSet<ROW> parentDataSet, KeySelector<ROW, KEY> partitionKeySelector, Partitioner<KEY> partitioner) {
        super(parentDataSet.getExecutionEnvironment());
        this.parentDataSet = parentDataSet;
        this.partitionKeySelector = partitionKeySelector;
        this.partitioner = partitioner;
    }

    @Override
    public Partition[] getPartitions() {
        return parentDataSet.getPartitions();
    }

    @Override
    public Iterator<ROW> compute(Partition partition) {
        try (ShuffleWriter<KEY, ROW> shuffleWriter = new ShuffleWriter<>(
                partition.getIndex(),
                partitionKeySelector,
                partitioner)) {

            Iterator<ROW> iterator = parentDataSet.compute(partition);
            shuffleWriter.write(iterator);
        }
        catch (IOException e) {
            throw new RuntimeException("shuffle map task failed", e);
        }
        return Iterators.empty();
    }

    public Partitioner<KEY> getPartitioner() {
        return partitioner;
    }

    @Override
    public DataSet<?> getParent() {
        return parentDataSet;
    }
}