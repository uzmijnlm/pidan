package com.github.pidan.batch.api;

import com.github.pidan.core.function.KeySelector;
import com.github.pidan.core.function.Partitioner;

import java.io.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static com.github.pidan.core.configuration.Constant.SHUFFLE_DATA_DIRECTORY;
import static com.github.pidan.core.configuration.Constant.SHUFFLE_FILE_PREFIX;

public class ShuffleWriter<KEY, ROW> implements Closeable {
    private final int stageId;
    private final int mapId;
    private final KeySelector<ROW, KEY> keySelector;
    private final Partitioner partitioner;

    public ShuffleWriter(int stageId, int mapId, KeySelector<ROW, KEY> keySelector, Partitioner partitioner) {
        this.stageId = stageId;
        this.mapId = mapId;
        this.keySelector = keySelector;
        this.partitioner = partitioner;
    }


    @Override
    public void close() throws IOException {

    }

    public void write(Iterator<ROW> iterator) throws IOException {
        Map<Integer, DataOutputStream> outputStreamMap = new HashMap<>();
        try {
            while (iterator.hasNext()) {
                ROW record = iterator.next();
                KEY key = keySelector.getKey(record);
                int partition = partitioner.getPartition(key);
                DataOutputStream dataOutputStream = outputStreamMap.get(partition);
                if (dataOutputStream == null) {
                    File file = getDataFile(stageId, mapId, partition);
                    if (!file.getParentFile().exists()) {
                        if (!file.getParentFile().mkdirs()) {
                            throw new RuntimeException("Make Directory "
                                    + file.getParentFile().getAbsolutePath() + " failed");
                        }
                    }
                    dataOutputStream = new DataOutputStream(new FileOutputStream(getDataFile(stageId, mapId, partition), false));
                    outputStreamMap.put(partition, dataOutputStream);
                }

                byte[] bytes = serialize(record);
                dataOutputStream.writeInt(bytes.length);
                dataOutputStream.write(bytes);
            }
        } finally {
            for (DataOutputStream dataOutputStream : outputStreamMap.values()) {
                dataOutputStream.writeInt(-1);
                dataOutputStream.close();
            }
        }

    }

    private byte[] serialize(ROW record) throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream os = new ObjectOutputStream(bos)) {
            os.writeObject(record);
            return bos.toByteArray();
        }
    }

    private File getDataFile(int shuffleId, int mapId, int reduceId) {
        return new File(SHUFFLE_DATA_DIRECTORY + SHUFFLE_FILE_PREFIX + shuffleId + "_" + mapId + "_" + reduceId + ".data");
    }
}
