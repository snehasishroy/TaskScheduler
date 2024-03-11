package com.snehasishroy.taskscheduler.callbacks;

import com.snehasishroy.taskscheduler.util.ZKUtils;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

@Slf4j
public class WorkersListener implements CuratorCacheListener {

  private final CuratorCache assignmentCache;
  private final CuratorFramework curator;

  public WorkersListener(CuratorCache assignmentCache, CuratorFramework curator) {
    this.assignmentCache = assignmentCache;
    this.curator = curator;
  }

  @Override
  public void event(Type type, ChildData oldData, ChildData data) {
    if (type == Type.NODE_CREATED) {
      log.info("New worker found {} ", data.getPath());
    } else if (type == Type.NODE_DELETED) {
      // notice we have to check oldData because data will be null
      log.info("Lost worker {}", oldData.getPath());
      String lostWorkerID = oldData.getPath().substring(oldData.getPath().lastIndexOf('/') + 1);
      // map of job ids -> job data, which was assigned to the lost worker
      Map<String, byte[]> assignableJobIds = new HashMap<>();
      assignmentCache.stream()
          .forEach(
              childData -> {
                String path = childData.getPath();
                int begin = path.indexOf('/') + 1;
                int end = path.indexOf('/', begin);
                String pathWorkerID = path.substring(begin, end);
                if (pathWorkerID.equals(lostWorkerID)) {
                  String jobID = path.substring(end + 1);
                  log.info("Found {} assigned to lost worker {}", jobID, lostWorkerID);
                  assignableJobIds.put(jobID, childData.getData());
                }
              });
      // Assuming atomic creation of assignment path and deletion of tasks path (using MultiOp), we
      // can safely assume that no entry exists under /jobs for the assigned tasks.
      // So we can simulate job creation by recreating an entry in the /jobs entry.
      assignableJobIds.forEach(
          (jobId, jobData) -> asyncCreateJob(ZKUtils.getJobsPath() + "/" + jobId, jobData));
    }
  }

  private void asyncCreateJob(String path, byte[] data) {
    try {
      curator
          .create()
          .idempotent()
          .withMode(CreateMode.PERSISTENT)
          .inBackground(
              new BackgroundCallback() {
                @Override
                public void processResult(CuratorFramework client, CuratorEvent event) {
                  switch (KeeperException.Code.get(event.getResultCode())) {
                    case OK -> {
                      log.info("Job repaired successfully for {}", path);
                    }
                    case CONNECTIONLOSS -> {
                      log.error(
                          "Lost connection to ZK while repairing job {}, retrying",
                          event.getPath());
                      asyncCreateJob(event.getPath(), (byte[]) event.getContext());
                    }
                    case NODEEXISTS -> {
                      log.warn("Job already exists for path {}", event.getPath());
                    }
                    default -> log.error("Unhandled event {}", event);
                  }
                }
              },
              data)
          .forPath(path, data);
    } catch (Exception e) {
      log.error("Error while repairing job {}", path, e);
      throw new RuntimeException(e);
    }
  }
}
