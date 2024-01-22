package com.snehasishroy.module;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class GuiceModule extends AbstractModule {

    @Provides
    public CuratorFramework curatorFramework() {
        CuratorFramework curatorFramework = CuratorFrameworkFactory.builder().namespace("TaskSchedulerV0") // namespace is a must to avoid conflicts in shared zk clusters
                .connectString("127.0.0.1:2181")
                .retryPolicy(new ExponentialBackoffRetry(10, 1))
                .build();
        curatorFramework.start();
        return curatorFramework;
    }
}
