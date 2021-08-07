package com.github.pidan.batch.runtime;

import com.github.pidan.batch.api.DataSet;
import com.github.pidan.core.TaskContext;
import com.github.pidan.core.partition.Partition;

public class ResultTask<ROW, OUT> implements Task<OUT> {
    private final int stageId;
    private final Partition partition;
    private final DataSet<ROW> dataSet;

    public ResultTask(int stageId, Partition partition, DataSet<ROW> dataSet) {
        this.stageId = stageId;
        this.partition = partition;
        this.dataSet = dataSet;
    }

    @Override
    public int getTaskId() {
        return partition.getIndex();
    }

    @Override
    public OUT runTask(TaskContext taskContext) {
        return null;
    }

    @Override
    public int getStageId() {
        return stageId;
    }
}
