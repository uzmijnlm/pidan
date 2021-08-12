package com.github.pidan.batch.runtime;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.Map;

public interface Task<OUT> extends Serializable {
    int getTaskId();

    OUT runTask(TaskContext taskContext);

    int getStageId();

    Map<Integer, Map<Integer, InetSocketAddress>> getDependMapTasks();

    Integer[] getDependencies();
}
