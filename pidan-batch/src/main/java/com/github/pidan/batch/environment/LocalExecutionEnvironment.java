package com.github.pidan.batch.environment;

import com.github.pidan.batch.api.DataSet;
import com.github.pidan.batch.api.ShuffleMapOperator;
import com.github.pidan.batch.runtime.ResultTask;
import com.github.pidan.batch.runtime.ShuffleMapTask;
import com.github.pidan.batch.runtime.Task;
import com.github.pidan.batch.runtime.TaskContext;
import com.github.pidan.batch.shuffle.*;
import com.github.pidan.core.function.MapFunction;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LocalExecutionEnvironment implements ExecutionEnvironment {

    private int nextStageId = 0;

    // Stage是从后往前生成，因此越后执行的Stage的id反而越小。通过下面的TreeSet让Stage对象按照执行顺序排序
    private final Set<Stage> stages = new TreeSet<>((stage1, stage2) -> stage2.getStageId() - stage1.getStageId());

    // key: Stage
    // value: 依赖的Stage的id数组
    private final Map<Stage, Integer[]> shuffleMapStageToDependencies = new HashMap<>();

    @Override
    public <ROW, OUT> List<OUT> runJob(DataSet<ROW> dataSet, MapFunction<Iterator<ROW>, OUT> function) {
        // 构造ResultStage
        ResultStage<ROW> resultStage = new ResultStage<>(dataSet, nextStageId++);
        // 根据依赖构造所有ShuffleMapStage
        createShuffleMapStage(resultStage);

        ShuffleClient shuffleClient = new LocalShuffleClient();
        // 执行ShuffleMapStage
        for (Stage stage : stages) {
            if (stage instanceof ShuffleMapStage) {
                Integer[] dependencies = shuffleMapStageToDependencies.get(stage);
                Stream.of(stage.getPartitions())
                        .map(partition -> CompletableFuture.runAsync(
                                () -> {
                                    Task<Integer> task = new ShuffleMapTask(
                                            stage.getStageId(),
                                            partition,
                                            stage.getFinalDataSet(),
                                            Collections.emptyMap(),
                                            dependencies);
                                    TaskContext taskContext = TaskContext.of(stage.getStageId(), dependencies, shuffleClient);
                                    task.runTask(taskContext);
                                })
                        )
                        .collect(Collectors.toList())
                        .forEach(CompletableFuture::join);
            }
        }

        // 执行ResultStage
        Integer[] dependencies = shuffleMapStageToDependencies.get(resultStage);
        return Stream.of(resultStage.getPartitions())
                .map(partition -> CompletableFuture.supplyAsync(
                        () -> {
                            Task<OUT> task = new ResultTask<>(
                                    resultStage.getStageId(),
                                    partition,
                                    function,
                                    (DataSet<ROW>) resultStage.getFinalDataSet(),
                                    Collections.emptyMap(),
                                    dependencies);
                            TaskContext taskContext = TaskContext.of(resultStage.getStageId(), dependencies, shuffleClient);
                            return task.runTask(taskContext);
                        }))
                .collect(Collectors.toList())
                .stream()
                .map(CompletableFuture::join)
                .collect(Collectors.toList());
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
}
