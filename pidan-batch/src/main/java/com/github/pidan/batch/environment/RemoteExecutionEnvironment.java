package com.github.pidan.batch.environment;

import com.github.pidan.batch.api.DataSet;
import com.github.pidan.batch.runtime.JobManager;
import com.github.pidan.batch.runtime.TaskManager;

import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

import static com.github.pidan.core.configuration.Constant.executorNum;

public class RemoteExecutionEnvironment implements ExecutionEnvironment {

    private final JobManager jobManager;

    private final TaskManager taskManager;

    {
        // 初始化JobManager并启动网络服务
        jobManager = new JobManager(executorNum);
        // 初始化TaskManager
        taskManager = new TaskManager(jobManager.getBindAddress());
        // 启动所有Executor
        taskManager.start();
        // 等待所有TaskExecutor上线
        jobManager.awaitAllExecutorRegistered();
    }


    @Override
    public <ROW, OUT> List<OUT> runJob(DataSet<ROW> dataSet, Function<Iterator<ROW>, OUT> foreach) {

        return null;
    }
}
