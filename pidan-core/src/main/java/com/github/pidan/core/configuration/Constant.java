package com.github.pidan.core.configuration;

public class Constant {
    public final static String SHUFFLE_DATA_DIRECTORY = "/tmp/shuffle/";
    public final static String SHUFFLE_FILE_PREFIX = "shuffle_";
    public final static boolean enableSortShuffle = false;
    public final static int executorNum = 1;
    public final static String DRIVER_MANAGER_ADDRESS = "driver.manager.address";
    public final static int JOB_MANAGER_PORT = 8080;
    public final static int TASK_MANAGER_PORT = 8081;
    public final static int SHUFFLE_SERVICE_PORT = 8082;
}
