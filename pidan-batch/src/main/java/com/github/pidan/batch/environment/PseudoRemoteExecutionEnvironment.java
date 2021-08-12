package com.github.pidan.batch.environment;

import com.github.pidan.batch.api.DataSet;
import com.github.pidan.batch.api.ShuffleMapOperator;
import com.github.pidan.batch.runtime.*;
import com.github.pidan.batch.runtime.event.TaskEvent;
import com.github.pidan.batch.shuffle.ResultStage;
import com.github.pidan.batch.shuffle.ShuffleMapStage;
import com.github.pidan.batch.shuffle.Stage;
import com.github.pidan.core.function.MapFunction;
import com.github.pidan.core.partition.Partition;

import java.net.InetSocketAddress;
import java.util.*;

import static com.github.pidan.core.configuration.Constant.executorNum;

public class PseudoRemoteExecutionEnvironment implements ExecutionEnvironment {

    private final JobManager jobManager;

    private final TaskManager taskManager;

    private int nextStageId = 0;

    // Stage是从后往前生成，因此越后执行的Stage的id反而越小。通过下面的TreeSet让Stage对象按照执行顺序排序
    private final Set<Stage> stages = new TreeSet<>((stage1, stage2) -> stage2.getStageId() - stage1.getStageId());

    // key: Stage
    // value: 依赖的Stage的id数组
    private final Map<Stage, Integer[]> shuffleMapStageToDependencies = new HashMap<>();

    public PseudoRemoteExecutionEnvironment() {
        // 初始化JobManager并启动网络服务
        jobManager = new JobManager(executorNum);
        // 初始化TaskManager
        taskManager = new TaskManager(jobManager.getBindAddress(), executorNum);
        // 启动所有Executor
        taskManager.start();
        // 等待所有TaskExecutor上线
        jobManager.awaitAllExecutorRegistered();
    }


    @Override
    public <ROW, OUT> List<OUT> runJob(DataSet<ROW> dataSet, MapFunction<Iterator<ROW>, OUT> foreach) {
        // 构造ResultStage
        ResultStage<ROW> resultStage = new ResultStage<>(dataSet, nextStageId++);
        // 根据依赖构造所有ShuffleMapStage
        createShuffleMapStage(resultStage);

        Object[] result = null;
        Map<Integer, Map<Integer, InetSocketAddress>> mapTaskNotes = new HashMap<>();
        for (Stage stage : stages) {
            Map<Integer, InetSocketAddress> mapTaskRunningExecutor = submitStage(stage, foreach, mapTaskNotes, shuffleMapStageToDependencies.get(stage));
            mapTaskNotes.put(stage.getStageId(), mapTaskRunningExecutor);
            if (stage instanceof ResultStage) {
                result = new Object[stage.getPartitions().length];
            }

            for (int taskDone = 0; taskDone < stage.getPartitions().length; ) {
                TaskEvent taskEvent = jobManager.awaitTaskEvent();
                taskDone++;
                if (stage instanceof ShuffleMapStage) {
                    TaskEvent.TaskSuccess taskSuccess = (TaskEvent.TaskSuccess) taskEvent;
                    Object shuffleMapStageResult = taskSuccess.getTaskResult();
                    if (shuffleMapStageResult instanceof Integer) {
                        if (((Integer) shuffleMapStageResult) == 0) {
                            continue;
                        }
                    }
                    throw new RuntimeException("Wrong result for ShuffleMapTask, taskId: " + taskSuccess.getTaskId());
                } else if (stage instanceof ResultStage) {
                    TaskEvent.TaskSuccess taskSuccess = (TaskEvent.TaskSuccess) taskEvent;
                    result[taskSuccess.getTaskId()] = taskSuccess.getTaskResult();
                }
            }
            if (stage instanceof ResultStage) {
                return Arrays.asList((OUT[]) result);
            }
        }
        throw new RuntimeException("Stage提交或运行异常，没有ResultStage的结果返回。");
    }

    private <ROW> void createShuffleMapStage(ResultStage<ROW> resultStage) {
        Deque<DataSet<?>> dataSetQueue = new LinkedList<>();
        Deque<ShuffleMapStage> stageQueue = new LinkedList<>();
        Stage currentStage = resultStage;
        DataSet<?> currentDataSet;
        dataSetQueue.offer(resultStage.getFinalDataSet());
        List<Integer> dependencies = new ArrayList<>();
        while (!dataSetQueue.isEmpty()) {
            currentDataSet = dataSetQueue.pop();
            if (currentDataSet instanceof ShuffleMapOperator) {
                shuffleMapStageToDependencies.put(currentStage, dependencies.toArray(new Integer[0]));
                dependencies.clear();
                currentStage = stageQueue.pop();
            }
            for (DataSet<?> dataSet : currentDataSet.getDependencies()) {
                if (dataSet instanceof ShuffleMapOperator) {
                    int stageId = nextStageId++;
                    dependencies.add(stageId);
                    stageQueue.offer(new ShuffleMapStage(dataSet, stageId));
                }
                dataSetQueue.push(dataSet);
            }
        }
        shuffleMapStageToDependencies.putIfAbsent(currentStage, dependencies.toArray(new Integer[0]));
        stages.addAll(shuffleMapStageToDependencies.keySet());
    }

    private <ROW, OUT> Map<Integer, InetSocketAddress> submitStage(Stage stage,
                                                                   MapFunction<Iterator<ROW>, OUT> foreach,
                                                                   Map<Integer, Map<Integer, InetSocketAddress>> dependMapTasks,
                                                                   Integer[] dependencies) {
        List<Task<?>> tasks = new ArrayList<>();
        if (stage instanceof ShuffleMapStage) {
            for (Partition partition : stage.getPartitions()) {
                Task<Integer> task = new ShuffleMapTask(
                        stage.getStageId(),
                        partition,
                        stage.getFinalDataSet(),
                        dependMapTasks,
                        dependencies);
                tasks.add(task);
            }
        } else {
            for (Partition partition : stage.getPartitions()) {
                Task<OUT> task = new ResultTask<>(
                        stage.getStageId(),
                        partition,
                        foreach,
                        (DataSet<ROW>) stage.getFinalDataSet(),
                        dependMapTasks,
                        dependencies);
                tasks.add(task);
            }
        }

        Map<Integer, InetSocketAddress> mapTaskRunningExecutor = new HashMap<>();
        tasks.forEach(task -> {
            InetSocketAddress address = jobManager.submitTask(task);
            mapTaskRunningExecutor.put(task.getTaskId(), address);
        });
        return mapTaskRunningExecutor;
    }

    @Override
    public void stop() {
        jobManager.stop();
        taskManager.stop();
    }

    @Override
    public void stopForcibly() {
        jobManager.stop();
        taskManager.stopForcibly();
    }
}
