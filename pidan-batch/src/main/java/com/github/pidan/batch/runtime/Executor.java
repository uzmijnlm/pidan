package com.github.pidan.batch.runtime;

import com.github.pidan.batch.runtime.event.TaskEvent;
import com.github.pidan.batch.shuffle.PseudoRemoteShuffleClient;
import com.github.pidan.batch.shuffle.ShuffleClient;

import java.io.Closeable;
import java.io.IOException;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Executor implements Closeable {
    private final ExecutorService pool = Executors.newFixedThreadPool(2);
    private final ExecutorBackend executorBackend;
    private final ShuffleManagerService shuffleService;


    public Executor(SocketAddress jobManagerAddress) throws InterruptedException, UnknownHostException {
        this.shuffleService = new ShuffleManagerService();
        this.executorBackend = new ExecutorBackend(this, jobManagerAddress);
        executorBackend.start(shuffleService.getShuffleServiceBindAddress());
    }

    public void join() throws InterruptedException {
        shuffleService.join();
    }

    public void runTask(Task<?> task) {
        pool.execute(() -> {
            try {
                ShuffleClient shuffleClient = new PseudoRemoteShuffleClient(task.getDependMapTasks());
                TaskContext taskContext = TaskContext.of(task.getStageId(), task.getDependencies(), shuffleClient);
                Object result = task.runTask(taskContext);
                TaskEvent event = TaskEvent.success(task.getTaskId(), result);
                executorBackend.updateState(event);
            } catch (Exception e) {
                throw new RuntimeException("Task run failed");
            }
        });
    }

    @Override
    public void close() {
        pool.shutdownNow();
        shuffleService.stop();
    }
}
