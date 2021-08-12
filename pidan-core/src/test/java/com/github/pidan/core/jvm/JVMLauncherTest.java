package com.github.pidan.core.jvm;

import com.github.pidan.core.util.SystemUtil;
import org.junit.Test;

import java.io.Serializable;

public class JVMLauncherTest {

    @Test
    public void testLauncher() {
        JVMLauncher<Integer> launcher = JVMLaunchers.<Integer>newJvm()
                .setEnvironment("driver.manager.address", "localhost:8080")
                .addUserJars(SystemUtil.getSystemClassLoaderJars())
                .setConsole(System.out::println)
                .setXmx("1024m")
                .build();

        launcher.startAsync((VmCallable<Integer>) () -> {
            AddressPrinter.main(new String[0]);
            return 0;
        });
    }


    static class AddressPrinter implements Serializable {

        public static void main(String[] args) {
            String address = System.getenv("driver.manager.address");
            System.out.println("Accept address: " + address);
        }

    }
}
