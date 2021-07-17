package com.github.pidan.batch.environment;

import com.github.pidan.batch.api.DataSet;
import com.github.pidan.batch.api.ShuffleMapOperator;
import com.github.pidan.batch.shuffle.ResultStage;
import com.github.pidan.batch.shuffle.ShuffleMapStage;
import com.github.pidan.core.Partition;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LocalExecutionEnvironment implements ExecutionEnvironment {

    @Override
    public <ROW, OUT> List<OUT> runJob(DataSet<ROW> dataSet, Function<Iterator<ROW>, OUT> function) {
        ResultStage<ROW> resultStage = runShuffleMapStage(dataSet);
        Partition[] partitions = dataSet.getPartitions();
        ExecutorService executors = Executors.newFixedThreadPool(partitions.length);
        try {
            return Stream.of(resultStage.getPartitions()).map(partition -> CompletableFuture.supplyAsync(() -> {
                Iterator<ROW> iterator = dataSet.compute(partition);
                return function.apply(iterator);
            }, executors)).collect(Collectors.toList()).stream()
                    .map(x -> x.join())
                    .collect(Collectors.toList());
        }
        finally {
            executors.shutdown();
        }
    }

    private <ROW> ResultStage<ROW> runShuffleMapStage(DataSet<ROW> dataSet)
    {
        List<DataSet<?>> shuffleMapOperators = new ArrayList<>();

        for (DataSet<?> parent = dataSet; parent != null; parent = parent.getParent()) {
            if (parent instanceof ShuffleMapOperator) {
                shuffleMapOperators.add(parent);
            }
        }

        List<ShuffleMapStage> stages = new ArrayList<>();
        for (int stageId = 0; stageId < shuffleMapOperators.size(); stageId++) {
            stages.add(new ShuffleMapStage(shuffleMapOperators.get(shuffleMapOperators.size() - stageId - 1)));
        }

        for (ShuffleMapStage stage : stages) {
            ExecutorService executors = Executors.newFixedThreadPool(dataSet.numPartitions());
            try {
                Stream.of(stage.getPartitions()).map(partition -> CompletableFuture.runAsync(() -> {
                    stage.getDataSet().compute(partition);
                }, executors)).collect(Collectors.toList()).forEach(CompletableFuture::join);
            }
            finally {
                executors.shutdown();
            }
        }

        return new ResultStage<>(dataSet);
    }
}
