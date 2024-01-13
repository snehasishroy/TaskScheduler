package com.snehasishroy.service;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.curator.framework.recipes.leader.CancelLeadershipException;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
@Getter
public class WorkerService implements LeaderSelectorListener, Closeable {
    public static final String WORKERS_ROOT = "/workers";
    public static final String JOBS_ROOT = "/jobs";
    public static final String ASSIGNMENT_ROOT = "/assignments";
    private final String name;
    private final LeaderSelector leaderSelector;
    Lock lock = new ReentrantLock();
    Condition condition = lock.newCondition();
    private final AtomicBoolean shouldStop = new AtomicBoolean(false);
    private final CuratorFramework curator;
    private CuratorCache workersCache;
    private CuratorCache jobsCache;
    private CuratorCache assignmentCache;
    private CuratorCacheListener workersListener;
    private CuratorCacheListener jobsListener;
    private CuratorCacheListener assignmentListener;

    public WorkerService(CuratorFramework curator, String path, String name) {
        this.name = name;
        this.curator = curator;
        leaderSelector = new LeaderSelector(curator, path, this);
        leaderSelector.setId(name);
        // the selection for this instance doesn't start until the leader selector is started
        // leader selection is done in the background so this call to leaderSelector.start() returns immediately
        leaderSelector.start();
        setup();
        watchAssignmentPath();
    }

    // TODO: Handle Reconnected State change, when the workers lose connection to the server, server will delete the ephemeral nodes,
    // client needs to handle this and recreate those nodes

    private void setup() {
        createIfExists(getWorkerPath(), CreateMode.EPHEMERAL);
        createIfExists(getJobsPath(), CreateMode.PERSISTENT);
        createIfExists(getAssignmentPath(), CreateMode.PERSISTENT);
    }

    private void createIfExists(String path, CreateMode mode) {
        log.info("Attempting creation of {}", path);
        try {
            Stat stat = curator.checkExists().forPath(path);
            if (stat == null) {
                log.info("Path does not exist, trying to create it {}", path);
                curator.create().creatingParentsIfNeeded().withMode(mode).forPath(path, name.getBytes());
            }
        } catch (Exception e) {
            log.error("Unable to create {} due to ", path, e);
            throw new RuntimeException(e);
        }
    }

    private void watchJobsAndWorkersPath() {
        workersCache = CuratorCache.build(curator, WORKERS_ROOT);
        jobsCache = CuratorCache.build(curator, JOBS_ROOT);
        log.info("Watching {}", getWorkerPath());
        workersCache.start();
        workersListener = new CuratorCacheListener() {
            @Override
            public void event(Type type, ChildData oldData, ChildData data) {
                if (type == Type.NODE_CREATED) {
                    log.info("found new worker {} ", data.getPath());
                } else if (type == Type.NODE_DELETED) {
                    // notice we have to check oldData because data will be null
                    log.info("Lost worker {}", oldData.getPath());
                }
            }
        };
        workersCache.listenable().addListener(workersListener);

        log.info("Watching {}", getJobsPath());
        jobsCache.start();
        jobsListener = new CuratorCacheListener() {
            @Override
            public void event(Type type, ChildData oldData, ChildData data) {
                if (type == Type.NODE_CREATED) {
                    log.info("found new job {} ", data.getPath());
                }
            }
        };
        jobsCache.listenable().addListener(jobsListener);
    }

    private void watchAssignmentPath() {
        assignmentCache = CuratorCache.build(curator, getAssignmentPath());
        log.info("Watching {}", getAssignmentPath());
        assignmentCache.start();
        assignmentListener = new CuratorCacheListener() {
            @Override
            public void event(Type type, ChildData oldData, ChildData data) {
                if (type == Type.NODE_CREATED) {
                    log.info("New assignment created {}", data.getPath());
                }
            }
        };
        assignmentCache.listenable().addListener(assignmentListener);
    }

    private void destroy() {
        log.info("Deleting worker path {}", getWorkerPath());
        try {
            curator.delete().forPath(getWorkerPath());
        } catch (Exception e) {
            log.info("Unable to delete {} due to ", getWorkerPath(), e);
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
    public void takeLeadership(CuratorFramework client) throws InterruptedException {
        // we are now the leader. This method should not return until we want to relinquish leadership
        log.info("{} is now the leader", name);
        // only the leader should setup watches
        watchJobsAndWorkersPath();
        while (true) {
            try {
                lock.lock();
                while (!shouldStop.get()) {
                    condition.await();
                }
                if (shouldStop.get()) {
                    log.info("{} is signalled to stop!", name);
                    leaderSelector.close();
                    return;
                }
            } finally {
                lock.unlock();
            }
        }
    }

    @Override
    public void stateChanged(CuratorFramework client, ConnectionState newState) {
        if (client.getConnectionStateErrorPolicy().isErrorState(newState)) {
            log.info("Something bad happened {}", newState);
            throw new CancelLeadershipException();
        }
    }

    public void stop() {
        // whatever was done in start(), need to do the reverse in stop()
        log.info("Sending stop signal to {}", name);
        destroy();
        shouldStop.compareAndSet(false, true);
        if (leaderSelector.hasLeadership()) {
            log.info("Giving up leadership {}", name);
            try {
                lock.lock();
                condition.signalAll();
            } finally {
                lock.unlock();
            }
        } else {
            // this is required to remove this node from the leader election set, otherwise it will get requeued as still this
            // node is part of the candidate set
            leaderSelector.close();
        }

    }

    public String getLeader() {
        try {
            return leaderSelector.getLeader().getId();
        } catch (Exception e) {
            log.info("Unable to get leader information due to ", e);
            return "NO_LEADER";
        }
    }

    private String getWorkerPath() {
        return WORKERS_ROOT + "/" + name;
    }

    private String getAssignmentPath() {
        return ASSIGNMENT_ROOT + "/" + name;
    }

    private String getJobsPath() {
        return JOBS_ROOT;
    }
}
