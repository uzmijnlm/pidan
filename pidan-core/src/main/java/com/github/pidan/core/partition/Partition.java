package com.github.pidan.core.partition;

import java.io.Serializable;

public interface Partition extends Serializable {
    int hashCode();
    int getIndex();
}