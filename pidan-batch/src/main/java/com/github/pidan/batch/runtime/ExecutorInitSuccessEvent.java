package com.github.pidan.batch.runtime;

import java.net.SocketAddress;

public class ExecutorInitSuccessEvent implements ExecutorEvent {
    private final SocketAddress shuffleServiceBindAddress;

    public ExecutorInitSuccessEvent(SocketAddress shuffleServiceBindAddress) {
        this.shuffleServiceBindAddress = shuffleServiceBindAddress;
    }

    public SocketAddress getShuffleServiceAddress() {
        return shuffleServiceBindAddress;
    }
}
