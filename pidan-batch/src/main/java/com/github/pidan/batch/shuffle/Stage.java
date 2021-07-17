package com.github.pidan.batch.shuffle;

import com.github.pidan.core.Partition;

public interface Stage {
    Partition[] getPartitions();
}
