package com.github.pidan.batch.shuffle;

import com.github.pidan.core.util.SerializableUtil;
import com.google.common.collect.Iterators;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Objects;
import java.util.stream.Stream;

import static com.github.pidan.core.configuration.Constant.SHUFFLE_DATA_DIRECTORY;
import static com.github.pidan.core.configuration.Constant.SHUFFLE_FILE_PREFIX;

public class ShuffleReader implements Serializable {
    private final int reduceId;
    private final int stageId;

    public ShuffleReader(int index, int stageId) {
        this.reduceId = index;
        this.stageId = stageId;
    }


    public Iterator<?> read() {
        File dataDir = new File(SHUFFLE_DATA_DIRECTORY);
        Iterator<Iterator<Object>> iterator = Stream.of(Objects.requireNonNull(dataDir.listFiles()))
                .filter(x -> x.getName().startsWith(SHUFFLE_FILE_PREFIX + stageId + "_") && x.getName().endsWith("_" + reduceId + ".data"))
                .map(file -> {
                    ArrayList<Object> out = new ArrayList<>();
                    try {
                        DataInputStream dataInputStream = new DataInputStream(new FileInputStream(file));
                        int totalSize = dataInputStream.readInt();
                        if (totalSize == 0) {
                            throw new RuntimeException("No data in file: " + file.getName());
                        }
                        do {
                            int length = dataInputStream.readInt();
                            byte[] bytes = new byte[length];
                            dataInputStream.read(bytes);
                            out.add(SerializableUtil.byteToObject(bytes));
                            totalSize = totalSize - length - 4;
                        } while (totalSize > 0);
                        dataInputStream.close();
                        return out.iterator();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }).iterator();

        return Iterators.concat(iterator);
    }
}
