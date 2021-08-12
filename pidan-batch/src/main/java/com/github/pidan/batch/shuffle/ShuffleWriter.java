package com.github.pidan.batch.shuffle;

import com.github.pidan.core.function.KeySelector;
import com.github.pidan.core.function.Partitioner;

import java.io.*;
import java.util.*;

import static com.github.pidan.core.configuration.Constant.SHUFFLE_DATA_DIRECTORY;
import static com.github.pidan.core.configuration.Constant.SHUFFLE_FILE_PREFIX;

public class ShuffleWriter<KEY, ROW> implements Closeable, Serializable {
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
    public void close() {

    }

    public void write(Iterator<ROW> iterator) throws IOException {
        Map<Integer, DataOutputStream> outputStreamMap = new HashMap<>();
        Map<Integer, List<byte[]>> partitionToByte = new HashMap<>();
        Map<Integer, Integer> partitionToTotalSize = new HashMap<>();
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
                    partitionToByte.put(partition, new ArrayList<>());
                    partitionToTotalSize.put(partition, 0);
                }

                byte[] bytes = serialize(record);
                partitionToByte.get(partition).add(bytes);
                partitionToTotalSize.put(partition, partitionToTotalSize.get(partition) + bytes.length + 4);
            }
            for (Map.Entry<Integer, List<byte[]>> entry : partitionToByte.entrySet()) {
                Integer partition = entry.getKey();
                List<byte[]> bytesList = entry.getValue();
                DataOutputStream dataOutputStream = outputStreamMap.get(partition);
                int totalSize = partitionToTotalSize.get(partition);
                dataOutputStream.writeInt(totalSize);
                for (byte[] bytes : bytesList) {
                    dataOutputStream.writeInt(bytes.length);
                    dataOutputStream.write(bytes);
                }
            }
        } finally {
            for (DataOutputStream dataOutputStream : outputStreamMap.values()) {
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
