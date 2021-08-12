package com.github.pidan.batch.api;

import com.github.pidan.batch.environment.ExecutionEnvironment;
import com.github.pidan.batch.runtime.TaskContext;
import com.github.pidan.core.partition.Partition;
import com.github.pidan.core.partition.TextFilePartition;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;

public class TextFileDataSource extends DataSet<String> {
    private final String path;

    public TextFileDataSource(ExecutionEnvironment env, String path) {
        super(env);
        this.path = path;
    }

    @Override
    public Partition[] getPartitions() {
        File file = new File(path);
        return new TextFilePartition[]{new TextFilePartition(0, file)};
    }

    @Override
    public Iterator<String> compute(Partition partition, TaskContext taskContext) {
        TextFilePartition textFilePartition = (TextFilePartition) partition;
        try {
            return Files.readAllLines(Paths.get(textFilePartition.getFile().toURI())).iterator();
        } catch (IOException e) {
            throw new RuntimeException("Read file error");
        }
    }
}
