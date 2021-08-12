package com.github.pidan.batch.runtime;

import com.github.pidan.batch.api.DataSet;
import com.github.pidan.core.function.MapFunction;
import com.github.pidan.core.partition.Partition;

import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.Map;

public class ResultTask<ROW, OUT> implements Task<OUT> {
    private final int stageId;
    private final Partition partition;
    private final DataSet<ROW> dataSet;
    private final MapFunction<Iterator<ROW>, OUT> foreach;
    private final Map<Integer, Map<Integer, InetSocketAddress>> dependMapTasks;
    private final Integer[] dependencies;

    public ResultTask(int stageId, Partition partition,
                      MapFunction<Iterator<ROW>, OUT> foreach, DataSet<ROW> dataSet,
                      Map<Integer, Map<Integer, InetSocketAddress>> dependMapTasks,
                      Integer[] dependencies) {
        this.stageId = stageId;
        this.partition = partition;
        this.foreach = foreach;
        this.dataSet = dataSet;
        this.dependMapTasks = dependMapTasks;
        this.dependencies = dependencies;
    }

    @Override
    public int getTaskId() {
        return partition.getIndex();
    }

    @Override
    public OUT runTask(TaskContext taskContext) {
        Iterator<ROW> iterator = dataSet.compute(partition, taskContext);
        return foreach.map(iterator);
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
