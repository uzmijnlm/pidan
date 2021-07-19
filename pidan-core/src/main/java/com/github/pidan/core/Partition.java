package com.github.pidan.core;

import java.io.Serializable;

public class Partition
        implements Serializable {
    private final int index;

    public Partition(int index) {
        this.index = index;
    }

    @Override
    public int hashCode() {
        return index;
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }

    public int getIndex() {
        return index;
    }
}