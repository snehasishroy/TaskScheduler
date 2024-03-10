package com.snehasishroy.taskscheduler.service;

import com.snehasishroy.taskscheduler.callbacks.AssignmentListener;
import com.snehasishroy.taskscheduler.callbacks.JobsListener;
import com.snehasishroy.taskscheduler.callbacks.WorkersListener;
import com.snehasishroy.taskscheduler.strategy.RoundRobinWorker;
import com.snehasishroy.taskscheduler.strategy.WorkerPickerStrategy;
import com.snehasishroy.taskscheduler.util.ZKUtils;
import java.io.Closeable;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.curator.framework.recipes.leader.CancelLeadershipException;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

@Slf4j
@Getter
public class WorkerService implements LeaderSelectorListener, Closeable {

  private final LeaderSelector leaderSelector;
  private final AtomicBoolean shouldStop = new AtomicBoolean(false);
  private final CuratorFramework curator;
  private final AtomicBoolean registrationRequired = new AtomicBoolean(true);
  private final WorkerPickerStrategy workerPickerStrategy;
  Lock lock = new ReentrantLock();
  Condition condition = lock.newCondition();
  ExecutorService executorService = Executors.newSingleThreadExecutor();
  private volatile String name;
  private CuratorCache workersCache;
  private CuratorCache jobsCache;
  private CuratorCache assignmentCache;
  private CuratorCacheListener workersListener;
  private CuratorCacheListener jobsListener;
  private CuratorCacheListener assignmentListener;

  /**
   * TODO: Need to implement a daemon service that periodically checks for stale jobs with no status
   * updates
   */
  public WorkerService(CuratorFramework curator, String path) {
    this.curator = curator;
    leaderSelector = new LeaderSelector(curator, path, this);
    // the selection for this instance doesn't start until the leader selector is started
    // leader selection is done in the background so this call to leaderSelector.start() returns
    // immediately
    leaderSelector.start();
    // this is important as it automatically handles failure scenarios i.e. starts leadership after
    // the reconnected state
    // https://www.mail-archive.com/user@curator.apache.org/msg00903.html
    leaderSelector.autoRequeue();
    setup();
    workerPickerStrategy = new RoundRobinWorker();
  }

  private void setup() {
    registerWorker();
    asyncCreate(ZKUtils.getJobsPath(), CreateMode.PERSISTENT, null);
    asyncCreate(ZKUtils.getAssignmentPath(name), CreateMode.PERSISTENT, null);
    asyncCreate(ZKUtils.STATUS_ROOT, CreateMode.PERSISTENT, null);
  }

  private void registerWorker() {
    if (registrationRequired.get()) {
      log.info("Attempting worker registration");
      name = UUID.randomUUID().toString();
      log.info("Generated a new random name to the worker {}", name);
      asyncCreate(ZKUtils.getWorkerPath(name), CreateMode.EPHEMERAL, registrationRequired);
      asyncCreate(ZKUtils.getAssignmentPath(name), CreateMode.PERSISTENT, null);
      watchAssignmentPath();
      // irrespective of whether this node is a leader or not, we need to watch the assignment path
    }
  }

  private void asyncCreate(String path, CreateMode mode, Object context) {
    // create the ZNode, no need to set any data with this znode
    try {
      curator
          .create()
          .idempotent()
          .creatingParentsIfNeeded()
          .withMode(mode)
          .inBackground(
              new BackgroundCallback() {
                @Override
                public void processResult(CuratorFramework client, CuratorEvent event) {
                  switch (KeeperException.Code.get(event.getResultCode())) {
                    case OK -> {
                      log.info("Path created successfully {}", event.getPath());
                      if (context != null) {
                        log.info("Setting the registration required field to false");
                        ((AtomicBoolean) context).set(false);
                      }
                    }
                    case CONNECTIONLOSS -> {
                      log.error(
                          "Lost connection to ZK while creating {}, retrying", event.getPath());
                      asyncCreate(event.getPath(), mode, context);
                    }
                    case NODEEXISTS -> {
                      log.warn("Node already exists for path {}", event.getPath());
                      if (context != null) {
                        log.info("Setting the registration required field to false");
                        ((AtomicBoolean) context).set(false);
                      }
                    }
                    default -> log.error("Unhandled event {}", event);
                  }
                }
              },
              context)
          .forPath(path);
    } catch (Exception e) {
      log.error("Unable to create {} due to ", path, e);
      throw new RuntimeException(e);
    }
  }

  // only the leader worker will watch for incoming jobs and changes to available workers
  private void watchJobsAndWorkersPath() {
    // Null check prevents starting duplicate cache and listeners in case the leadership is
    // reacquired
    if (workersCache == null) {
      workersCache = CuratorCache.build(curator, ZKUtils.WORKERS_ROOT);
      workersCache.start();
      log.info("Watching workers {}", ZKUtils.getWorkerPath(name));
      workersListener = new WorkersListener(assignmentCache, curator);
      workersCache.listenable().addListener(workersListener);
    }
    if (jobsCache == null) {
      jobsCache = CuratorCache.build(curator, ZKUtils.JOBS_ROOT);
      log.info("Watching jobs {}", ZKUtils.getJobsPath());
      jobsCache.start();
      jobsListener = new JobsListener(curator, workersCache, workerPickerStrategy);
      jobsCache.listenable().addListener(jobsListener);
    }
  }

  private void watchAssignmentPath() {
    // No need to check for null here because once a session is reconnected after a loss
    // we need to start the assignment listener on the new worker id
    assignmentCache = CuratorCache.build(curator, ZKUtils.getAssignmentPath(name));
    log.info("Watching {}", ZKUtils.getAssignmentPath(name));
    assignmentCache.start();
    assignmentListener = new AssignmentListener(curator);
    assignmentCache.listenable().addListener(assignmentListener);
  }

  private void destroy() {
    log.info("Deleting worker path {}", ZKUtils.getWorkerPath(name));
    try {
      curator.delete().forPath(ZKUtils.getWorkerPath(name));
    } catch (Exception e) {
      log.info("Unable to delete {} due to ", ZKUtils.getWorkerPath(name), e);
    }
    log.info("Removing workers listener");
    workersCache.listenable().removeListener(workersListener);
    workersCache.close();
    log.info("Removing jobs listener");
    jobsCache.listenable().removeListener(jobsListener);
    jobsCache.close();
  }

  @Override
  public void close() {
    leaderSelector.close();
  }

  @Override
  public void takeLeadership(CuratorFramework client) {
    // we are now the leader. This method should not return until we want to relinquish leadership,
    // which will only happen, if someone has signalled us to stop
    log.info("{} is now the leader", name);
    // only the leader should watch the jobs and workers path
    watchJobsAndWorkersPath();
    lock.lock();
    try {
      while (!shouldStop.get()) {
        condition.await();
      }
      if (shouldStop.get()) {
        log.warn("{} is signalled to stop!", name);
        leaderSelector.close();
      }
    } catch (InterruptedException e) { // this is propagated from cancel leadership election
      log.error("Thread is interrupted, need to exit the leadership", e);
    } finally {
      // finally is called before the method return
      lock.unlock();
    }
  }

  @Override
  public void stateChanged(CuratorFramework client, ConnectionState newState) {
    if (newState == ConnectionState.RECONNECTED) {
      log.error("Reconnected to ZK, Received {}", newState);
      // no need to start the leadership again as it is auto requeued but worker re-registration is
      // still required which will create a ZNode in /workers and /assignments path
      registerWorker();
    } else if (newState == ConnectionState.LOST) {
      log.error(
          "Connection lost to ZK, session has been expired, giving up leadership {}", newState);
      registrationRequired.set(true);
      // This is required as the assignment cache listens on the {worker id} which is ephemeral
      // In case of a lost session, it's guaranteed that the {worker id} would have expired
      // Once the session is reconnected, we need to set up the assignment listener once again
      log.info("Removing the watcher set on the assignment listener");
      assignmentCache.listenable().removeListener(assignmentListener);
      assignmentCache.close();
      // throwing this specific exception would cause the current thread to interrupt and would
      // cause and InterruptedException
      throw new CancelLeadershipException();
    } else if (newState == ConnectionState.SUSPENDED) {
      log.error("Connection has been suspended to ZK, giving up leadership {}", newState);
      throw new CancelLeadershipException();
    }
  }

  public void stop() {
    // whatever was done in start(), need to do the reverse in stop()
    log.warn("Sending stop signal to {}", name);
    destroy();
    shouldStop.compareAndSet(false, true);
    if (leaderSelector.hasLeadership()) {
      log.warn("Giving up leadership {}", name);
      try {
        lock.lock();
        condition.signalAll();
      } finally {
        lock.unlock();
      }
    } else {
      // this is required to remove this node from the leader election set, otherwise it will get
      // requeued as still this node is part of the candidate set
      leaderSelector.close();
    }
  }

  public Optional<String> getLeader() {
    try {
      return Optional.of(leaderSelector.getLeader().getId());
    } catch (Exception e) {
      log.error("Unable to get leader information due to ", e);
      return Optional.empty();
    }
  }
}
