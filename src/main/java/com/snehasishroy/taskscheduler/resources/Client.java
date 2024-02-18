package com.snehasishroy.taskscheduler.resources;

import com.google.inject.Inject;
import com.snehasishroy.taskscheduler.JobDetail;
import com.snehasishroy.taskscheduler.service.ClientService;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.QueryParam;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;

import java.io.Serializable;

@Slf4j
@Path("/v1/client")
public class Client {
    private final ClientService clientService;

    @Inject
    public Client(CuratorFramework curator) {
        this.clientService = new ClientService(curator);
    }

    @POST
    public String createSumTask(@QueryParam("first") int a, @QueryParam("second") int b) {
        Runnable jobDetail = (Runnable & Serializable)(() -> System.out.println("Sum of " + a + " and " + b + " is " + (a + b)));
        return clientService.registerJob(jobDetail);
    }
}
