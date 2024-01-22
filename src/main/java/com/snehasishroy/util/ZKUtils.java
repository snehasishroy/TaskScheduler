package com.snehasishroy.util;

public class ZKUtils {
    public static final String WORKERS_ROOT = "/workers";
    public static final String JOBS_ROOT = "/jobs";
    public static final String ASSIGNMENT_ROOT = "/assignments";

    public static String getWorkerPath(String name) {
        return WORKERS_ROOT + "/" + name;
    }

    public static String getAssignmentPath(String name) {
        return ASSIGNMENT_ROOT + "/" + name;
    }

    public static String getJobsPath() {
        return JOBS_ROOT;
    }
}
