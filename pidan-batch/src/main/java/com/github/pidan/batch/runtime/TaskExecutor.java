package com.github.pidan.batch.runtime;


import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;

import static com.github.pidan.core.configuration.Constant.DRIVER_MANAGER_ADDRESS;
import static com.github.pidan.core.configuration.Constant.JOB_MANAGER_PORT;

public class TaskExecutor {
    public static void main(String[] args) throws InterruptedException, UnknownHostException {
        String driverManagerAddressString = System.getenv(DRIVER_MANAGER_ADDRESS);
        String[] split = driverManagerAddressString.split(":");
        SocketAddress driverManagerAddress = InetSocketAddress.createUnresolved(split[0], JOB_MANAGER_PORT);
        Executor executor = new Executor(driverManagerAddress);
        executor.join();
    }
}
