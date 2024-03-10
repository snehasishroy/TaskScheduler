package com.snehasishroy.taskscheduler.callbacks;

import com.snehasishroy.taskscheduler.strategy.WorkerPickerStrategy;
import com.snehasishroy.taskscheduler.util.ZKUtils;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

@Slf4j
public class JobAssigner implements Runnable {

  private final CuratorFramework curator;
  private final String jobID;
  private final CuratorCache workersCache;
  private final WorkerPickerStrategy workerPickerStrategy;
  private final byte[] jobData;
  private String workerName;

  public JobAssigner(
      String jobID,
      byte[] jobData,
      CuratorFramework curator,
      CuratorCache workersCache,
      WorkerPickerStrategy workerPickerStrategy) {
    this.jobID = jobID;
    this.curator = curator;
    this.workersCache = workersCache;
    this.workerPickerStrategy = workerPickerStrategy;
    this.jobData = jobData;
  }

  @Override
  public void run() {
    List<ChildData> workers =
        workersCache.stream()
            .filter(childData -> (childData.getPath().length() > ZKUtils.WORKERS_ROOT.length()))
            .toList();
    ChildData chosenWorker = workerPickerStrategy.evaluate(workers);
    workerName = ZKUtils.extractNode(chosenWorker.getPath());
    log.info(
        "Found total workers {}, Chosen worker index {}, worker name {}",
        workers.size(),
        chosenWorker,
        workerName);
    asyncCreateAssignment();
  }

  private void asyncCreateAssignment() {
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
                      log.info(
                          "Assignment created successfully for JobID {} with WorkerID {}",
                          jobID,
                          workerName);
                      log.info(
                          "Performing async deletion of {}", ZKUtils.getJobsPath() + "/" + jobID);
                      asyncDelete(ZKUtils.getJobsPath() + "/" + jobID);
                    }
                    case CONNECTIONLOSS -> {
                      log.error(
                          "Lost connection to ZK while creating {}, retrying", event.getPath());
                      asyncCreateAssignment();
                    }
                    case NODEEXISTS -> {
                      log.warn("Assignment already exists for path {}", event.getPath());
                    }
                    case NONODE -> {
                      log.error("Trying to create an assignment for a worker which does not exist {}", event);
                    }
                    default -> log.error("Unhandled event {} ", event);
                  }
                }
              })
          .forPath(ZKUtils.ASSIGNMENT_ROOT + "/" + workerName + "/" + jobID, jobData);
      // Storing the job data along with the assignment, so that the respective worker need not
      // perform an additional call to get the job details.
      // This also simplifies the design - because we can delete the /jobs/{jobID} path once the
      // assignment  is completed - indicating that if an entry is present under /jobs, it's
      // assignment is not yet done.
      // This makes the recovery/reconciliation process much easier. Once a leader is elected, it
      // has to only perform liveliness check for the existing assignments.
      // TODO: Use MultiOp to perform assignment and deletion atomically
    } catch (Exception e) {
      log.error("Error while creating assignment for {} with {}", jobID, workerName, e);
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
