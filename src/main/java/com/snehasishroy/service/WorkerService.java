package com.snehasishroy.service;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.curator.framework.recipes.leader.CancelLeadershipException;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import java.io.Closeable;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
    private PersistentNode workerNode;
    ExecutorService executorService = Executors.newSingleThreadExecutor();

    public WorkerService(CuratorFramework curator, String path, String name) {
        this.name = name;
        this.curator = curator;
        leaderSelector = new LeaderSelector(curator, path, this);
        leaderSelector.setId(name);
        // the selection for this instance doesn't start until the leader selector is started
        // leader selection is done in the background so this call to leaderSelector.start() returns immediately
        leaderSelector.start();
        // this is important as it automatically handles failure scenarios i.e. starts leadership after the reconnected state
        // https://www.mail-archive.com/user@curator.apache.org/msg00903.html
        leaderSelector.autoRequeue();
        setup();
        watchAssignmentPath();
    }

    // TODO: Handle Reconnected State change, when the workers lose connection to the server, server will delete the ephemeral nodes,
    // client needs to handle this and recreate those nodes

    private void setup() {
        workerNode = createPersistentNode(getWorkerPath(), CreateMode.EPHEMERAL);
        createIfExists(getJobsPath(), CreateMode.PERSISTENT);
        createIfExists(getAssignmentPath(), CreateMode.PERSISTENT);
    }

    private PersistentNode createPersistentNode(String path, CreateMode mode) {
        log.info("Attempting creation of {}", path);
        PersistentNode persistentNode = new PersistentNode(curator, mode, false, path, new byte[]{}, true);
        persistentNode.start();
        return persistentNode;
    }

    private void createIfExists(String path, CreateMode mode) {
        // create the ZNode, no need to set any data with this znode
        try {
            curator.create().idempotent().creatingParentsIfNeeded().withMode(mode).inBackground(new BackgroundCallback() {
                @Override
                public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
                    switch (KeeperException.Code.get(event.getResultCode())) {
                        case OK -> {
                            log.info("Path created successfully {}", event.getPath());
                        }
                        case CONNECTIONLOSS -> {
                            log.info("Lost connection while creating {}, retrying", event.getPath());
                            createIfExists(event.getPath(), mode);
                        }
                        case NODEEXISTS -> {
                            log.info("Node already exists for path {}", event.getPath());
                        }
                    }
                }
            }).forPath(path);
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
                if (type == Type.NODE_CREATED && data.getPath().length() > JOBS_ROOT.length()) {
                    log.info("found new job {} ", data.getPath());
                    executorService.submit(new JobHandler(data.getPath()));
                }
            }
        };
        jobsCache.listenable().addListener(jobsListener);
    }

    class JobHandler implements Runnable {

        private String jobID;

        JobHandler(String jobID) {
            this.jobID = jobID;
        }

        @Override
        public void run() {
            workersCache.stream()
                    .filter(childData -> (childData.getPath().length() > WORKERS_ROOT.length()))
                    .forEach(childData -> log.info("Available workers are {}", childData.getPath()));

        }
    }

    private void watchAssignmentPath() {
        assignmentCache = CuratorCache.build(curator, getAssignmentPath());
        log.info("Watching {}", getAssignmentPath());
        assignmentCache.start();
//        assignmentListener = CuratorCacheListener.builder()
//                .forCreates()
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
        try {
            workerNode.close();
        } catch (IOException e) {
            log.error("Unable to close worker node", e);
        }
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
        // only the leader should setup watches
        watchJobsAndWorkersPath();
        lock.lock();
        try {
            while (!shouldStop.get()) {
                condition.await();
            }
            if (shouldStop.get()) {
                log.info("{} is signalled to stop!", name);
                leaderSelector.close();
            }
        } catch (InterruptedException e) {
            log.error("Thread is interrupted, need to exit the leadership", e);
        } finally {
            // finally is called before the method return
            lock.unlock();
        }
    }

    @Override
    public void stateChanged(CuratorFramework client, ConnectionState newState) {
        if (client.getConnectionStateErrorPolicy().isErrorState(newState)) {
            log.info("Something bad happened {}", newState);
            throw new CancelLeadershipException();
        } else if (newState == ConnectionState.RECONNECTED) {
            // no need to recreate the worker path because persistent node will take care of it
            // if persistent node is not used, then it becomes tricky to recreate the node
            log.info("Reconnected to ZK");
            // no need to start the leadership again as it is auto requeued
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
