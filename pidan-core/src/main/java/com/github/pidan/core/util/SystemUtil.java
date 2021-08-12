package com.github.pidan.core.util;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;

public class SystemUtil {
    public static List<URL> getSystemClassLoaderJars() {
        ClassLoader classLoader = ClassLoader.getSystemClassLoader();
        if (classLoader instanceof URLClassLoader) {
            return java.util.Arrays.asList(((URLClassLoader) classLoader).getURLs());
        } else {
            throw new RuntimeException("get system classLoader error");
        }
    }
}
