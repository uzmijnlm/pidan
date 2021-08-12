package com.github.pidan.core.jvm;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Objects.requireNonNull;

public class VmFuture<R extends Serializable> {
    private final Process process;
    private final Future<VmResult<R>> future;

    public VmFuture(AtomicReference<Process> processAtomic, Callable<VmResult<R>> callable) throws JVMException, InterruptedException {
        requireNonNull(processAtomic, "process is null");
        ExecutorService service = Executors.newFixedThreadPool(4);
        this.future = service.submit(callable);
        service.shutdown();

        while (processAtomic.get() == null) {
            if (future.isDone()) {
                try {
                    R r = future.get().get();
                    throw new JVMException("Async failed! future.isDone() result:" + r);
                } catch (ExecutionException e) {
                    // this throws ExecutionException
                    throw new JVMException(e.getCause());
                }
            }

            TimeUnit.MILLISECONDS.sleep(1);
        }
        this.process = processAtomic.get();
    }

    public Process getVmProcess() {
        return process;
    }

    public int getPid() {
        Process process = getVmProcess();
        String system = process.getClass().getName();
        if ("java.lang.UNIXProcess".equals(system)) {
            try {
                Field field = process.getClass().getDeclaredField("pid");
                field.setAccessible(true);
                return (int) field.get(process);
            } catch (NoSuchFieldException | IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
        throw new UnsupportedOperationException("Only support for UNIX and Linux systems pid, Your " + system + " is Windows ?");
    }

    public R get()
            throws JVMException, InterruptedException, ExecutionException {
        return future.get().get();
    }

    public R get(long timeout, TimeUnit unit)
            throws JVMException, InterruptedException, TimeoutException, ExecutionException {
        return future.get(timeout, unit).get();
    }

    public boolean isRunning() {
        if (future.isDone()) {
            return false;
        }
        return getVmProcess().isAlive();
    }

    public void cancel() {
        getVmProcess().destroy();
    }

    public void cancelForcibly() {
        getVmProcess().destroyForcibly();
    }
}
