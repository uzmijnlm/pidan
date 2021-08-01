package com.github.pidan.core;

import java.util.stream.Stream;

public interface TaskContext {
    static TaskContext of(int stageId, Integer[] dependencies) {
        int[] intArray = Stream.of(dependencies).mapToInt(x -> x).toArray();
        return of(stageId, intArray);
    }

    static TaskContext of(int stageId, int[] dependencies) {
        return new TaskContext() {
            @Override
            public int getStageId() {
                return stageId;
            }

            @Override
            public int[] getDependStages() {
                return dependencies;
            }
        };
    }

    int getStageId();
    int[] getDependStages();
}
