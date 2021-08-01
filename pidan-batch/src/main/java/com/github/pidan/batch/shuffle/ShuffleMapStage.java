package com.github.pidan.batch.shuffle;

import com.github.pidan.batch.api.DataSet;
import com.github.pidan.core.Partition;
import com.github.pidan.core.TaskContext;

import java.util.Iterator;

public class ShuffleMapStage implements Stage {
    private final DataSet<?> dataSet;
    private final int stageId;

    public ShuffleMapStage(DataSet<?> dataSet, int stageId) {
        this.dataSet = dataSet;
        this.stageId = stageId;
    }

    @Override
    public Partition[] getPartitions() {
        return dataSet.getPartitions();
    }

    public Iterator<?> compute(Partition partition, TaskContext taskContext) {
        return dataSet.compute(partition, taskContext);
    }

    @Override
    public int getStageId() {
        return stageId;
    }

    @Override
    public DataSet<?> getFinalDataSet() {
        return dataSet;
    }
}
