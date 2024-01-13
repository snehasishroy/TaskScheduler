package com.snehasishroy.resources;

import com.snehasishroy.service.WorkerService;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.UUID;

// https://medium.com/@nadinCodeHat/rest-api-naming-conventions-and-best-practices-1c4e781eb6a5
// Use nouns to represent resources and HTTP methods are then used to perform actions on those resources
@Path("/v1/workers")
public class Worker {
    private static final String PATH = "/leader";
    WorkerService worker;
    private final CuratorFramework client;

    public Worker() {
        client = CuratorFrameworkFactory.builder()
                .namespace("TaskSchedulerV0") // namespace is a must to avoid conflicts in shared zk clusters
                .connectString("127.0.0.1:2181")
                .retryPolicy(new ExponentialBackoffRetry(10, 1))
                .build();
        client.start();
        initWorker();
    }

    public void initWorker() {
        worker = new WorkerService(client, PATH, UUID.randomUUID().toString());
    }

    @DELETE
    @Path("/{id}")
    public void stopWorker(@PathParam("id") String id) {
        worker.stop();
    }

    @GET
    @Path("/leader")
    public String getLeaderId() {
        return worker.getLeader();
    }
}
