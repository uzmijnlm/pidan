package com.github.pidan.batch.runtime;

import com.github.pidan.core.TaskContext;

import java.io.Serializable;

public interface Task<OUT> extends Serializable {
    int getTaskId();

    OUT runTask(TaskContext taskContext);

    int getStageId();
}
