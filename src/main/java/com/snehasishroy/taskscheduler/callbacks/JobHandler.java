package com.snehasishroy.taskscheduler.callbacks;

import com.snehasishroy.taskscheduler.util.ZKUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import java.util.List;

@Slf4j
public class JobHandler implements Runnable {

    private final CuratorFramework curator;
    private final String jobID;
    private final CuratorCache workersCache;
    private String workerName;

    public JobHandler(String jobID, CuratorFramework curator, CuratorCache workersCache) {
        this.jobID = jobID;
        this.curator = curator;
        this.workersCache = workersCache;
    }

    @Override
    public void run() {
        List<ChildData> workers = workersCache.stream()
                .filter(childData -> (childData.getPath().length() > ZKUtils.WORKERS_ROOT.length()))
                .toList();
        int chosenWorker = (int) (Math.random() * workers.size());
        workerName = ZKUtils.extractNode(workers.get(chosenWorker).getPath());
        log.info("Found total workers {}, Chosen worker index {}, worker name {}", workers.size(), chosenWorker, workerName);
        createAssignment();
    }

    private void createAssignment() {
        try {
            curator.create().idempotent().withMode(CreateMode.PERSISTENT).inBackground(new BackgroundCallback() {
                @Override
                public void processResult(CuratorFramework client, CuratorEvent event) {
                    switch (KeeperException.Code.get(event.getResultCode())) {
                        case OK -> {
                            log.info("Assignment created successfully for {} with {}", jobID, workerName);
//                            asyncDelete(ZKUtils.JOBS_ROOT + "/" + jobID);
                        }
                        case CONNECTIONLOSS -> {
                            log.info("Lost connection to ZK while creating {}, retrying", event.getPath());
                            createAssignment();
                        }
                        case NODEEXISTS -> {
                            log.info("Assignment already exists for path {}", event.getPath());
                        }
                    }
                }
            }).forPath(ZKUtils.ASSIGNMENT_ROOT + "/" + workerName + "/" + jobID);
        } catch (Exception e) {
            log.error("Error while creating assignment for {} with {}", jobID, workerName, e);
            throw new RuntimeException(e);
        }
    }

    private void asyncDelete(String path) {
        // create the ZNode, no need to set any data with this znode
        try {
            curator.delete().idempotent().guaranteed().inBackground(new BackgroundCallback() {
                @Override
                public void processResult(CuratorFramework client, CuratorEvent event) {
                    switch (KeeperException.Code.get(event.getResultCode())) {
                        case OK -> {
                            log.info("Path deleted successfully {}", event.getPath());
                        }
                        case CONNECTIONLOSS -> {
                            log.info("Lost connection to ZK while deleting {}, retrying", event.getPath());
                            asyncDelete(event.getPath());
                        }
                    }
                }
            }).forPath(path);
        } catch (Exception e) {
            log.error("Unable to delete {} due to ", path, e);
            throw new RuntimeException(e);
        }
    }
}