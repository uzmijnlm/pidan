package com.github.pidan.batch.shuffle;

import com.github.pidan.batch.api.DataSet;
import com.github.pidan.batch.runtime.TaskContext;
import com.github.pidan.core.partition.Partition;

import java.io.Serializable;
import java.util.Iterator;

public interface Stage extends Serializable {
    Partition[] getPartitions();

    Iterator<?> compute(Partition partition, TaskContext taskContext);

    int getStageId();

    DataSet<?> getFinalDataSet();
}
