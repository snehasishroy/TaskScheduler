package com.snehasishroy.taskscheduler.callbacks;

import com.snehasishroy.taskscheduler.strategy.WorkerPickerStrategy;
import com.snehasishroy.taskscheduler.util.ZKUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class JobsListener implements CuratorCacheListener {
    private final CuratorFramework curator;
    private final CuratorCache workersCache;
    private final ExecutorService executorService;
    private final WorkerPickerStrategy workerPickerStrategy;

    public JobsListener(CuratorFramework curator, CuratorCache workersCache, WorkerPickerStrategy workerPickerStrategy) {
        this.curator = curator;
        this.workersCache = workersCache;
        executorService = Executors.newSingleThreadExecutor();
        this.workerPickerStrategy = workerPickerStrategy;
    }

    @Override
    public void event(Type type, ChildData oldData, ChildData data) {
        if (type == Type.NODE_CREATED && data.getPath().length() > ZKUtils.JOBS_ROOT.length()) {
            String jobContents = new String(data.getData());
            log.info("job contents {}", jobContents);
            String jobID = ZKUtils.extractNode(data.getPath());
            log.info("found new job {}, passing it to executor service", jobID);
            // an executor service is used in order to avoid blocking the watcher thread as the job execution can be time consuming
            // and we don't want to skip handling new jobs during that time
            executorService.submit(new JobHandler(jobID, curator, workersCache, workerPickerStrategy));
        }
    }
}
