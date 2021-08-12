package com.github.pidan.batch.runtime;

import com.github.pidan.core.jvm.JVMLauncher;
import com.github.pidan.core.jvm.JVMLaunchers;
import com.github.pidan.core.jvm.VmCallable;
import com.github.pidan.core.jvm.VmFuture;
import com.github.pidan.core.util.SystemUtil;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;

import static com.github.pidan.core.configuration.Constant.DRIVER_MANAGER_ADDRESS;

public class TaskManager {

    private final SocketAddress driverManagerAddress;
    private final int executorNum;
    private final List<VmFuture<Integer>> vms;

    public TaskManager(SocketAddress driverManagerAddress, int executorNum) {
        this.driverManagerAddress = driverManagerAddress;
        this.executorNum = executorNum;
        this.vms = new ArrayList<>(executorNum);
    }

    public void start() {
        for (int i = 0; i < executorNum; i++) {
            JVMLauncher<Integer> launcher = JVMLaunchers.<Integer>newJvm()
                    .setEnvironment(DRIVER_MANAGER_ADDRESS, driverManagerAddress.toString())
                    .addUserJars(SystemUtil.getSystemClassLoaderJars())
                    .setConsole(System.out::print)
                    .setXmx("1024m")
                    .build();
            VmFuture<Integer> vmFuture = launcher.startAsync((VmCallable<Integer>) () -> {
                TaskExecutor.main(new String[0]);
                return 0;
            });
            vms.add(vmFuture);
        }
        Runtime.getRuntime().addShutdownHook(new Thread(() -> vms.forEach(VmFuture::cancel)));
    }

    public void stop() {
        vms.forEach(VmFuture::cancel);
    }

    public void stopForcibly() {
        vms.forEach(VmFuture::cancelForcibly);
    }
}
