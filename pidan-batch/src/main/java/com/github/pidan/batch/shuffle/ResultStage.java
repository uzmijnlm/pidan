package com.github.pidan.batch.shuffle;

import com.github.pidan.batch.api.DataSet;
import com.github.pidan.core.Partition;

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
    public Iterator<ROW> compute(Partition partition) {
        return dataSet.compute(partition, () -> stageId);
    }
}
