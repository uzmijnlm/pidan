package com.github.pidan.batch.shuffle;

import com.github.pidan.core.Partition;

import java.util.Iterator;

public interface Stage {
    Partition[] getPartitions();

    Iterator<?> compute(Partition partition);
}
