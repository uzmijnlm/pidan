package com.github.pidan.batch.runtime;


import java.net.InetSocketAddress;
import java.net.SocketAddress;

public class TaskExecutor {
    public static void main(String[] args) {
        String address = "localhost";
        int port = 0;
        SocketAddress driverManagerAddress = InetSocketAddress.createUnresolved(address, port);

        Executor executor = new Executor(driverManagerAddress);
        executor.join();
    }
}
