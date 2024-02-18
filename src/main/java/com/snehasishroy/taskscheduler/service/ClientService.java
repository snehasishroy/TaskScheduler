package com.snehasishroy.taskscheduler.service;

import com.snehasishroy.taskscheduler.util.ZKUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.UUID;

@Slf4j
public class ClientService {
    private final CuratorFramework curator;

    public ClientService(CuratorFramework curator) {
        this.curator = curator;
    }

    public String registerJob(Runnable jobDetail) {
        String jobId = UUID.randomUUID().toString();
        syncCreate(ZKUtils.getJobsPath() + "/" + jobId, jobDetail);
        return jobId;
    }

    private void syncCreate(String path, Runnable runnable) {
        // create the ZNode along with the data
        try {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
            objectOutputStream.writeObject(runnable);
            curator.create().idempotent().withMode(CreateMode.PERSISTENT).forPath(path, byteArrayOutputStream.toByteArray());
        } catch (Exception e) {
            log.error("Unable to create {} due to ", path, e);
            throw new RuntimeException(e);
        }
    }
}
