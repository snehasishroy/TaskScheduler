package com.snehasishroy.taskscheduler.callbacks;

import com.snehasishroy.taskscheduler.util.ZKUtils;
import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

@Slf4j
public class AssignmentListener implements CuratorCacheListener {
  private final CuratorFramework curator;
  private final ExecutorService executorService;

  public AssignmentListener(CuratorFramework curator) {
    this.curator = curator;
    this.executorService = Executors.newFixedThreadPool(10);
  }

  @Override
  public void event(Type type, ChildData oldData, ChildData data) {
    if (type == Type.NODE_CREATED) {
      if (data.getPath().indexOf('/', 1) == data.getPath().lastIndexOf('/')) {
        // This filters out the root path /assignment/{worker-id} which does not contains any job id
        return;
      }
      String jobId = data.getPath().substring(data.getPath().lastIndexOf('/') + 1);
      log.info("Assignment found for job id {}", jobId);

      try {
        byte[] bytes = data.getData();
        ObjectInputStream objectInputStream =
            new ObjectInputStream(new ByteArrayInputStream(bytes));
        Runnable jobDetail = (Runnable) objectInputStream.readObject();
        log.info("Deserialized the JobId {} to {}", jobId, jobDetail);
        CompletableFuture<Void> future = CompletableFuture.runAsync(jobDetail, executorService);
        // Actual execution of the job will be performed in a separate thread to avoid blocking of
        // watcher thread
        log.info("Job submitted for execution");
        // once the job has been executed, we need to ensure the assignment is deleted and the
        // status of job has been updated. Currently there is no guarantee that post the execution,
        // this cleanup happens.
        // TODO: Implement a daemon service which performs cleanup
        future.thenAcceptAsync(__ -> asyncCreate(jobId, data.getPath()), executorService);
      } catch (Exception e) {
        log.error("Unable to fetch data for job id {}", jobId, e);
      }
    }
  }

  private void asyncCreate(String jobId, String assignmentPath) {
    log.info("JobID {} has been executed, moving on to update its status", jobId);
    // create the ZNode, no need to set any data with this znode
    try {
      curator
          .create()
          .withTtl(ZKUtils.STATUS_TTL_MILLIS)
          .creatingParentsIfNeeded()
          .withMode(CreateMode.PERSISTENT_WITH_TTL)
          .inBackground(
              new BackgroundCallback() {
                @Override
                public void processResult(CuratorFramework client, CuratorEvent event) {
                  switch (KeeperException.Code.get(event.getResultCode())) {
                    case OK -> {
                      log.info("Status updated successfully {}", event.getPath());
                      log.info("Performing deletion of assignment path {}", assignmentPath);
                      asyncDelete(assignmentPath);
                    }
                    case CONNECTIONLOSS -> {
                      log.error(
                          "Lost connection to ZK while creating {}, retrying", event.getPath());
                      asyncCreate(jobId, assignmentPath);
                    }
                    case NODEEXISTS -> {
                      log.warn("Node already exists for path {}", event.getPath());
                    }
                    default -> log.error("Unhandled event {}", event);
                  }
                }
              })
          .forPath(ZKUtils.getStatusPath(jobId), "Completed".getBytes());
    } catch (Exception e) {
      log.error("Unable to create {} due to ", ZKUtils.getStatusPath(jobId), e);
      throw new RuntimeException(e);
    }
  }

  private void asyncDelete(String path) {
    // delete the provided ZNode
    try {
      curator
          .delete()
          .idempotent()
          .guaranteed()
          .inBackground(
              new BackgroundCallback() {
                @Override
                public void processResult(CuratorFramework client, CuratorEvent event) {
                  switch (KeeperException.Code.get(event.getResultCode())) {
                    case OK -> {
                      log.info("Path deleted successfully {}", event.getPath());
                    }
                    case CONNECTIONLOSS -> {
                      log.info(
                          "Lost connection to ZK while deleting {}, retrying", event.getPath());
                      asyncDelete(event.getPath());
                    }
                    default -> log.error("Unhandled event {}", event);
                  }
                }
              })
          .forPath(path);
    } catch (Exception e) {
      log.error("Unable to delete {} due to ", path, e);
      throw new RuntimeException(e);
    }
  }
}
