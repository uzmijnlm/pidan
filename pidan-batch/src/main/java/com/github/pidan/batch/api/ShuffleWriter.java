package com.github.pidan.batch.api;

import com.github.pidan.batch.shuffle.ShuffleManager;
import com.github.pidan.core.function.KeySelector;
import com.github.pidan.core.function.Partitioner;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

public class ShuffleWriter<KEY, ROW> implements Closeable {
    private final int mapId;
    private final KeySelector<ROW, KEY> keySelector;
    private final Partitioner<KEY> partitioner;

    public ShuffleWriter(int mapId, KeySelector<ROW, KEY> keySelector, Partitioner<KEY> partitioner) {
        this.mapId = mapId;
        this.keySelector = keySelector;
        this.partitioner = partitioner;
    }



    @Override
    public void close() throws IOException {

    }

    public void write(Iterator<ROW> iterator) {
        while (iterator.hasNext()) {
            ROW record = iterator.next();
            KEY key = keySelector.getKey(record);
            int partition = partitioner.getPartition(key);
            ShuffleManager.write(mapId, partition, record);
        }
    }
}
