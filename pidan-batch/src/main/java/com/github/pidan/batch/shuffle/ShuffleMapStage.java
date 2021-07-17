package com.github.pidan.batch.shuffle;

import com.github.pidan.batch.api.DataSet;
import com.github.pidan.core.Partition;

public class ShuffleMapStage implements Stage {
    private final DataSet<?> dataSet;

    public ShuffleMapStage(DataSet<?> dataSet) {
        this.dataSet = dataSet;
    }

    @Override
    public Partition[] getPartitions() {
        return dataSet.getPartitions();
    }

    public DataSet<?> getDataSet() {
        return dataSet;
    }
}
