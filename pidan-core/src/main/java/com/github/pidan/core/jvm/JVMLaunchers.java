package com.github.pidan.core.jvm;

import java.io.Serializable;
import java.net.URL;
import java.util.*;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

public class JVMLaunchers {
    private JVMLaunchers() {
    }

    public static class VmBuilder<T extends Serializable> {
        private Consumer<String> consoleHandler;
        private final List<URL> tmpJars = new ArrayList<>();
        private final List<String> otherVmOps = new ArrayList<>();
        private final Map<String, String> environment = new HashMap<>();

        public VmBuilder<T> setConsole(Consumer<String> consoleHandler) {
            this.consoleHandler = requireNonNull(consoleHandler, "consoleHandler is null");
            return this;
        }

        public VmBuilder<T> addUserJars(Collection<URL> jars) {
            tmpJars.addAll(jars);
            return this;
        }

        public VmBuilder<T> setXmx(String xmx) {
            otherVmOps.add("-Xmx" + xmx);
//            // 通过设置如下参数可以实现远程debug
//            otherVmOps.add("-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=9000");
            return this;
        }

        public VmBuilder<T> setEnvironment(Map<String, String> env) {
            this.environment.putAll(requireNonNull(env, "env is null"));
            return this;
        }

        public VmBuilder<T> setEnvironment(String key, String value) {
            this.environment.put(key, value);
            return this;
        }

        public JVMLauncher<T> build() {
            return new JVMLauncher<>(consoleHandler, tmpJars, otherVmOps, environment);
        }
    }

    public static <T extends Serializable> VmBuilder<T> newJvm() {
        return new VmBuilder<>();
    }
}
