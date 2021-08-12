package com.github.pidan.core.jvm;

import com.github.pidan.core.util.SerializableUtil;

import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;

public class JVMLauncher<R extends Serializable> {

    private final Collection<URL> userJars;
    private final Consumer<String> consoleHandler;
    private final List<String> otherVmOps;
    private final Map<String, String> environment;

    JVMLauncher(
            Consumer<String> consoleHandler,
            Collection<URL> userJars,
            List<String> otherVmOps,
            Map<String, String> environment) {
        this.userJars = userJars;
        this.consoleHandler = consoleHandler;
        this.otherVmOps = otherVmOps;
        this.environment = environment;
    }

    public VmFuture<R> startAsync(VmCallable<R> task) throws JVMException {
        try {
            byte[] bytes = SerializableUtil.serialize(task);
            AtomicReference<Process> processAtomic = new AtomicReference<>();
            return new VmFuture<>(processAtomic, () -> this.startAndGetByte(processAtomic, bytes));
        } catch (IOException | InterruptedException e) {
            throw new JVMException(e);
        }
    }

    private VmResult<R> startAndGetByte(AtomicReference<Process> processAtomic, byte[] bytes) throws Exception {
        try (ServerSocket sock = new ServerSocket()) {
            sock.bind(new InetSocketAddress(InetAddress.getLocalHost(), 0));

            ProcessBuilder builder = new ProcessBuilder(buildMainArg(sock.getLocalPort(), otherVmOps))
                    .redirectErrorStream(true);

            builder.environment().putAll(environment);

            Process process = builder.start();
            processAtomic.set(process);

            try (OutputStream os = new BufferedOutputStream(process.getOutputStream())) {
                os.write(bytes);
            }

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream(), UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    consoleHandler.accept(line);
                }
            }

            sock.setSoTimeout(100);
            try (Socket socketClient = sock.accept();
                 InputStream inputStream = socketClient.getInputStream()) {
                return SerializableUtil.byteToObject(inputStream);
            } catch (SocketTimeoutException e) {
                if (process.isAlive()) {
                    process.destroy();
                }
                throw new JVMException("Jvm child process abnormal exit, exit code " + process.exitValue(), e);
            }
        }
    }

    private String getUserAddClasspath() {
        return userJars.stream()
                .map(URL::getPath)
                .collect(Collectors.joining(File.pathSeparator));
    }

    private List<String> buildMainArg(int port, List<String> otherVmOps) {
        File java = new File(new File(System.getProperty("java.home"), "bin"), "java");
        List<String> ops = new ArrayList<>();
        ops.add(java.toString());

        ops.addAll(otherVmOps);

        ops.add("-classpath");
        //ops.add(System.getProperty("java.class.path"));
        String userSdkJars = getUserAddClasspath(); //编译时还需要 用户的额外jar依赖

        ops.add(System.getProperty("java.class.path") + File.pathSeparator + userSdkJars);

        String javaLibPath = System.getProperty("java.library.path");
        if (javaLibPath != null) {
            ops.add("-Djava.library.path=" + javaLibPath);
        }
        ops.add(JVMLauncher.class.getCanonicalName()); //子进程会启动这个类 进行编译
        ops.add(Integer.toString(port));
        return ops;
    }

    public static void main(String[] args) throws Exception {
        System.out.println("vm start ok ...");
        VmResult<? extends Serializable> future;

        try (ObjectInputStream ois = new ObjectInputStream(System.in)) {
            System.out.println("vm start init ok ...");
            VmCallable<? extends Serializable> task = (VmCallable<? extends Serializable>) ois.readObject();
            future = new VmResult<>(task.call());
        } catch (Throwable e) {
            future = new VmResult<>(new RuntimeException(e));
            System.out.println("vm task run error");
        }

        try (OutputStream out = chooseOutputStream(args)) {
            out.write(SerializableUtil.serialize(future));
            System.out.println("vm exiting ok ...");
        }
    }

    static OutputStream chooseOutputStream(String[] args)
            throws IOException
    {
        if (args.length > 0) {
            int port = Integer.parseInt(args[0]);
            Socket sock = new Socket();
            sock.connect(new InetSocketAddress(InetAddress.getLocalHost(), port));
            return sock.getOutputStream();
        }
        else {
            return System.out;
        }
    }
}
