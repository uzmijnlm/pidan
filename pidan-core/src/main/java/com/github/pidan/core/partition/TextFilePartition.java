package com.github.pidan.core.partition;

import java.io.File;

public class TextFilePartition implements Partition {
    private final int index;
    private final File file;

    public TextFilePartition(int index, File file) {
        this.index = index;
        this.file = file;
    }

    @Override
    public int getIndex() {
        return index;
    }

    public File getFile() {
        return file;
    }
}
