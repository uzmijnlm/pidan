package com.github.pidan.batch.shuffle;

import com.github.pidan.batch.api.DataSet;
import com.github.pidan.batch.runtime.TaskContext;
import com.github.pidan.core.partition.Partition;

import java.util.Iterator;

public class ResultStage<ROW> implements Stage {

    private final DataSet<ROW> dataSet;
    private final int stageId;

    public ResultStage(DataSet<ROW> dataSet, int stageId) {
        this.dataSet = dataSet;
        this.stageId = stageId;
    }

    @Override
    public Partition[] getPartitions() {
        return dataSet.getPartitions();
    }

    @Override
    public Iterator<ROW> compute(Partition partition, TaskContext taskContext) {
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
