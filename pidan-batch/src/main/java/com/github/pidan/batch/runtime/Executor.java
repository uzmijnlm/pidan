package com.github.pidan.batch.runtime;

import java.net.SocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Executor {
    private final ExecutorService pool =  Executors.newFixedThreadPool(2);
    private final ExecutorBackend executorBackend;
    private final ShuffleManagerService shuffleService;


    public Executor(SocketAddress jobManagerAddress) {
        this.shuffleService = new ShuffleManagerService();
        this.executorBackend = new ExecutorBackend(this, jobManagerAddress);
        executorBackend.start(shuffleService.getShuffleServiceBindAddress());
    }

    public void join() {
        shuffleService.join();
    }

    public void runTask(Task<?> task) {

    }
}
