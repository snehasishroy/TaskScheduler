package com.snehasishroy.taskscheduler.resources;

import com.google.inject.Inject;
import com.snehasishroy.taskscheduler.util.ZKUtils;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;

import java.util.UUID;

@Path("/v1/jobs")
@Slf4j
public class Job {

    private final CuratorFramework curator;

    @Inject
    public Job(CuratorFramework curator) {
        this.curator = curator;
    }

    @POST
    public void createJob(String jobData) {
        log.info("Received job {}", jobData);
        try {
            curator.create().withMode(CreateMode.PERSISTENT).forPath(ZKUtils.getJobsPath() + "/" + UUID.randomUUID(), jobData.getBytes());
        } catch (Exception e) {
            log.error("Unable to submit job", e);
            throw new RuntimeException(e);
        }
    }
}
