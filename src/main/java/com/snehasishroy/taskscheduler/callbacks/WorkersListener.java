package com.snehasishroy.taskscheduler.callbacks;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;

@Slf4j
public class WorkersListener implements CuratorCacheListener {
    @Override
    public void event(Type type, ChildData oldData, ChildData data) {
        if (type == Type.NODE_CREATED) {
            log.info("found new worker {} ", data.getPath());
        } else if (type == Type.NODE_DELETED) {
            // notice we have to check oldData because data will be null
            log.info("Lost worker {}", oldData.getPath());
        }
    }
}
