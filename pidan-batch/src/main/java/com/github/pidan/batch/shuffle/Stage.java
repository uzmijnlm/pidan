package com.github.pidan.batch.shuffle;

import com.github.pidan.batch.api.DataSet;
import com.github.pidan.core.Partition;
import com.github.pidan.core.TaskContext;

import java.util.Iterator;

public interface Stage {
    Partition[] getPartitions();

    Iterator<?> compute(Partition partition, TaskContext taskContext);

    int getStageId();

    DataSet<?> getFinalDataSet();
}
