package com.github.pidan.core.function;

public class HashPartitioner<KEY> extends Partitioner<KEY> {

    @Override
    public int getPartition(KEY key, int numPartitions)
    {
        return (key.hashCode() & 2147483647) % numPartitions;
    }
}