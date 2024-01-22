package com.snehasishroy.service;

import com.snehasishroy.callbacks.JobHandler;
import com.snehasishroy.util.ZKUtils;
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
import org.apache.curator.framework.state.ConnectionState;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import java.io.Closeable;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
@Getter
public class WorkerService implements LeaderSelectorListener, Closeable {

    private final LeaderSelector leaderSelector;
    private final AtomicBoolean shouldStop = new AtomicBoolean(false);
    private final CuratorFramework curator;
    private final AtomicBoolean registrationRequired = new AtomicBoolean(true);
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

    public WorkerService(CuratorFramework curator, String path) {
        this.curator = curator;
        leaderSelector = new LeaderSelector(curator, path, this);
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
        registerWorker();
        asyncCreate(ZKUtils.getJobsPath(), CreateMode.PERSISTENT, null);
        asyncCreate(ZKUtils.getAssignmentPath(name), CreateMode.PERSISTENT, null);
    }

    private void registerWorker() {
        if (registrationRequired.get()) {
            log.info("Attempting worker registration");
            name = UUID.randomUUID().toString();
            log.info("Generated a new random name to the worker {}", name);
            asyncCreate(ZKUtils.getWorkerPath(name), CreateMode.EPHEMERAL, registrationRequired);
        }
    }

    private void asyncCreate(String path, CreateMode mode, Object context) {
        // create the ZNode, no need to set any data with this znode
        try {
            curator.create().idempotent().creatingParentsIfNeeded().withMode(mode).inBackground(new BackgroundCallback() {
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
                            log.info("Lost connection to ZK while creating {}, retrying", event.getPath());
                            asyncCreate(event.getPath(), mode, context);
                        }
                        case NODEEXISTS -> {
                            log.info("Node already exists for path {}", event.getPath());
                        }
                    }
                }
            }, context).forPath(path);
        } catch (Exception e) {
            log.error("Unable to create {} due to ", path, e);
            throw new RuntimeException(e);
        }
    }

    private void watchJobsAndWorkersPath() {
        workersCache = CuratorCache.build(curator, ZKUtils.WORKERS_ROOT);
        jobsCache = CuratorCache.build(curator, ZKUtils.JOBS_ROOT);
        log.info("Watching {}", ZKUtils.getWorkerPath(name));
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

        log.info("Watching {}", ZKUtils.getJobsPath());
        jobsCache.start();
        jobsListener = new CuratorCacheListener() {
            @Override
            public void event(Type type, ChildData oldData, ChildData data) {
                if (type == Type.NODE_CREATED && data.getPath().length() > ZKUtils.JOBS_ROOT.length()) {
                    String jobContents = new String(data.getData());
                    log.info("job contents {}", jobContents);
                    String jobID = ZKUtils.extractNode(data.getPath());
                    log.info("found new job {} ", jobID);
                    executorService.submit(new JobHandler(jobID, curator, workersCache));
                }
            }
        };
        jobsCache.listenable().addListener(jobsListener);
    }

    private void watchAssignmentPath() {
        assignmentCache = CuratorCache.build(curator, ZKUtils.getAssignmentPath(name));
        log.info("Watching {}", ZKUtils.getAssignmentPath(name));
        assignmentCache.start();
//        assignmentListener = CuratorCacheListener.builder()
//                .forCreates()
        assignmentListener = new CuratorCacheListener() {
            @Override
            public void event(Type type, ChildData oldData, ChildData data) {
                if (type == Type.NODE_CREATED) {
                    log.info("New job assignment {}", data.getPath());
                }
            }
        };
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
            // no need to recreate the worker path because persistent node will take care of it
            // if persistent node is not used, then it becomes tricky to recreate the node
            log.info("Reconnected to ZK, Received {}", newState);
            // no need to start the leadership again as it is auto requeued
            registerWorker();
        } else if (newState == ConnectionState.LOST) {
            log.info("Connection lost to ZK, session has been expired, giving up leadership {}", newState);
            registrationRequired.set(true);
            throw new CancelLeadershipException();
        } else if (newState == ConnectionState.SUSPENDED) {
            log.info("Connection has been suspended to ZK, giving up leadership {}", newState);
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


}
