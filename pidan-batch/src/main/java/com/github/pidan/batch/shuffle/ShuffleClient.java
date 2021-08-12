package com.github.pidan.batch.shuffle;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

public interface ShuffleClient extends Closeable {

    @Override
    default void close() throws IOException {

    }

    Iterator<?> read(int index, int stageId);
}
