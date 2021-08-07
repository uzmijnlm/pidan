package com.github.pidan.batch.runtime;

import com.github.pidan.batch.api.DataSet;
import com.github.pidan.core.TaskContext;
import com.github.pidan.core.partition.Partition;

public class ShuffleMapTask implements Task<Object> {
    private final int stageId;
    private final Partition partition;
    private final DataSet<?> dataSet;

    public ShuffleMapTask(int stageId, Partition partition, DataSet<?> dataSet) {
        this.stageId = stageId;
        this.partition = partition;
        this.dataSet = dataSet;
    }

    @Override
    public int getTaskId() {
        return partition.getIndex();
    }

    @Override
    public Object runTask(TaskContext taskContext) {
        return dataSet.compute(partition, taskContext);
    }

    @Override
    public int getStageId() {
        return stageId;
    }
}
