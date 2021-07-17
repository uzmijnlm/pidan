package com.github.pidan.batch.shuffle;

import com.google.common.collect.Iterators;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;

public class ShuffleManager {
    private static final Map<Integer, Map<Integer, Queue<?>>> reduceTaskMap = new ConcurrentHashMap<>(128);

    public static <ROW> void write(int mapId, int partition, ROW record) {
        Map<Integer, Queue<?>> mapIdToQueue = reduceTaskMap.computeIfAbsent(partition, k -> new ConcurrentHashMap<>());
        Queue<ROW> queue = (Queue<ROW>) mapIdToQueue.computeIfAbsent(mapId, k -> new LinkedBlockingDeque<>());
        queue.add(record);
    }

    public static Iterator<?> read(int index) {
        Map<Integer, Queue<?>> mapIdToQueue = reduceTaskMap.get(index);
        Collection<Queue<?>> values = mapIdToQueue.values();
        Iterator<? extends Iterator<?>> iterators = values.stream().map(queue -> queue.stream().iterator()).iterator();
        return Iterators.concat(iterators);
    }
}
