package com.github.pidan.core.partition;

public class ShuffledPartition implements Partition {
    private final int index;

    public ShuffledPartition(int index) {
        this.index = index;
    }

    @Override
    public int getIndex() {
        return index;
    }
}
