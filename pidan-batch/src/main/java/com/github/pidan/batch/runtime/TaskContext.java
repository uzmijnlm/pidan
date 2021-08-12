package com.github.pidan.batch.runtime;

import com.github.pidan.batch.shuffle.ShuffleClient;

import java.util.stream.Stream;

public interface TaskContext {

    static TaskContext of(int stageId, Integer[] dependencies, ShuffleClient shuffleClient) {
        int[] intArray = Stream.of(dependencies).mapToInt(x -> x).toArray();
        return of(stageId, intArray, shuffleClient);
    }

    static TaskContext of(int stageId, int[] dependencies, ShuffleClient shuffleClient) {
        return new TaskContext() {
            @Override
            public int getStageId() {
                return stageId;
            }

            @Override
            public int[] getDependStages() {
                return dependencies;
            }

            @Override
            public ShuffleClient getShuffleClient() {
                return shuffleClient;
            }
        };
    }

    int getStageId();
    int[] getDependStages();
    ShuffleClient getShuffleClient();
}
