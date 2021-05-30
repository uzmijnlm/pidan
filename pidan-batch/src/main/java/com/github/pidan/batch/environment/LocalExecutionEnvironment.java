package com.github.pidan.batch.environment;

import com.github.pidan.batch.api.DataSet;
import com.github.pidan.core.Partition;
import com.github.pidan.core.function.Foreach;

import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

public class LocalExecutionEnvironment implements ExecutionEnvironment {

    @Override
    public <ROW> void runJob(DataSet<ROW> dataSet, Foreach<Iterator<ROW>> foreach) {
        Partition[] partitions = dataSet.getPartitions();
        ExecutorService executors = Executors.newFixedThreadPool(partitions.length);
        try {
            Stream.of(partitions).parallel().map(partition -> CompletableFuture.runAsync(() -> {
                Iterator<ROW> iterator = dataSet.compute(partition);
                foreach.apply(iterator);
            }, executors)).forEach(CompletableFuture::join);
        }
        finally {
            executors.shutdown();
        }
    }
}
