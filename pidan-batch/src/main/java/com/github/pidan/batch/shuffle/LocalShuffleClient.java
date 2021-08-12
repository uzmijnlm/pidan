package com.github.pidan.batch.shuffle;

import java.util.Iterator;

public class LocalShuffleClient implements ShuffleClient {

    public Iterator<?> read(int index, int stageId) {
        ShuffleReader shuffleReader = new ShuffleReader(index, stageId);
        return shuffleReader.read();
    }

}
