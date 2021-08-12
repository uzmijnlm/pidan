package com.github.pidan.batch.runtime;

import com.github.pidan.batch.api.DataSet;
import com.github.pidan.core.partition.Partition;

import java.net.InetSocketAddress;
import java.util.Map;

public class ShuffleMapTask implements Task<Integer> {
    private final int stageId;
    private final Partition partition;
    private final DataSet<?> dataSet;
    private final Map<Integer, Map<Integer, InetSocketAddress>> dependMapTasks;
    private final Integer[] dependencies;

    public ShuffleMapTask(int stageId, Partition partition, DataSet<?> dataSet,
                          Map<Integer, Map<Integer, InetSocketAddress>> dependMapTasks,
                          Integer[] dependencies) {
        this.stageId = stageId;
        this.partition = partition;
        this.dataSet = dataSet;
        this.dependMapTasks = dependMapTasks;
        this.dependencies = dependencies;
    }

    @Override
    public int getTaskId() {
        return partition.getIndex();
    }

    @Override
    public Integer runTask(TaskContext taskContext) {
        dataSet.compute(partition, taskContext);
        return 0;
    }

    @Override
    public int getStageId() {
        return stageId;
    }

    @Override
    public Map<Integer, Map<Integer, InetSocketAddress>> getDependMapTasks() {
        return dependMapTasks;
    }

    @Override
    public Integer[] getDependencies() {
        return dependencies;
    }
}
