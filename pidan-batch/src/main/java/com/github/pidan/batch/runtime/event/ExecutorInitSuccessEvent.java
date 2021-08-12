package com.github.pidan.batch.runtime.event;

import java.net.InetSocketAddress;

public class ExecutorInitSuccessEvent implements ExecutorEvent {
    private final InetSocketAddress shuffleServiceBindAddress;

    public ExecutorInitSuccessEvent(InetSocketAddress shuffleServiceBindAddress) {
        this.shuffleServiceBindAddress = shuffleServiceBindAddress;
    }

    public InetSocketAddress getShuffleServiceAddress() {
        return shuffleServiceBindAddress;
    }
}
