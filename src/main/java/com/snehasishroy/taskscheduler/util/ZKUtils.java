package com.snehasishroy.taskscheduler.util;

import java.util.concurrent.TimeUnit;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ZKUtils {
  public static final String WORKERS_ROOT = "/workers";
  public static final String JOBS_ROOT = "/jobs";
  public static final String ASSIGNMENT_ROOT = "/assignments";
  public static final String STATUS_ROOT = "/status";
  public static final long STATUS_TTL_MILLIS = TimeUnit.MINUTES.toMillis(10);

  public static String getWorkerPath(String name) {
    return WORKERS_ROOT + "/" + name;
  }

  public static String getAssignmentPath(String name) {
    return ASSIGNMENT_ROOT + "/" + name;
  }

  public static String getJobsPath() {
    return JOBS_ROOT;
  }

  public static String extractNode(String workerPath) {
    int start = workerPath.lastIndexOf('/');
    return workerPath.substring(start + 1);
  }

  public static String getStatusPath(String jobId) {
    return STATUS_ROOT + "/" + jobId;
  }
}
