package com.snehasishroy.callbacks;

import com.snehasishroy.util.ZKUtils;
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

    public JobsListener(CuratorFramework curator, CuratorCache workersCache) {
        this.curator = curator;
        this.workersCache = workersCache;
        executorService = Executors.newSingleThreadExecutor();
    }

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
}
