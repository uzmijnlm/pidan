package com.github.pidan.core.function;

public class HashPartitioner<KEY> extends Partitioner<KEY> {

    private final int numPartitions;

    public HashPartitioner(int numPartitions)
    {
        this.numPartitions = numPartitions;
    }

    @Override
    public int numPartitions()
    {
        return numPartitions;
    }

    @Override
    public int getPartition(KEY key)
    {
        return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}