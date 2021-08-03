package com.github.pidan.core.function;

import java.io.Serializable;

public abstract class Partitioner implements Serializable {
    public abstract int numPartitions();

    public abstract int getPartition(Object key);
}