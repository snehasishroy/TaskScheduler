package com.snehasishroy.taskscheduler.callbacks;

import com.snehasishroy.taskscheduler.util.ZKUtils;
import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;

@Slf4j
public class AssignmentListener implements CuratorCacheListener {
  private final CuratorFramework curator;
  private final ExecutorService executorService;

  public AssignmentListener(CuratorFramework curator) {
    this.curator = curator;
    this.executorService = Executors.newSingleThreadExecutor();
  }

  @Override
  public void event(Type type, ChildData oldData, ChildData data) {
    if (type == Type.NODE_CREATED) {
      String jobId = data.getPath().substring(data.getPath().lastIndexOf('/') + 1);
      log.info("Assignment found for job id {}", jobId);

      log.info("Fetching job details for job id {}", jobId);
      try {
        byte[] bytes = curator.getData().forPath(ZKUtils.getJobsPath() + "/" + jobId);
        ObjectInputStream objectInputStream =
            new ObjectInputStream(new ByteArrayInputStream(bytes));
        Runnable jobDetail = (Runnable) objectInputStream.readObject();
        log.info("Deserialized the runnable {}", jobDetail);
        executorService.submit(jobDetail);
        log.info("Job submitted");
      } catch (Exception e) {
        log.error("Unable to fetch data for job id {}", jobId, e);
      }
    }
  }
}
