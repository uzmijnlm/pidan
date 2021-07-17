package com.github.pidan.batch.shuffle;

import com.github.pidan.batch.api.DataSet;
import com.github.pidan.core.Partition;

public class ResultStage<ROW> implements Stage {

    private final DataSet<ROW> dataSet;

    public ResultStage(final DataSet<ROW> dataSet)
    {
        this.dataSet = dataSet;
    }

    @Override
    public Partition[] getPartitions() {
        return dataSet.getPartitions();
    }
}
