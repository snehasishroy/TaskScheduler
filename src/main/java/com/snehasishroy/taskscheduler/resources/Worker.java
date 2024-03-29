package com.snehasishroy.taskscheduler.resources;

import com.google.inject.Inject;
import com.snehasishroy.taskscheduler.service.WorkerService;
import com.snehasishroy.taskscheduler.util.ZKUtils;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import org.apache.curator.framework.CuratorFramework;

// https://medium.com/@nadinCodeHat/rest-api-naming-conventions-and-best-practices-1c4e781eb6a5
// Use nouns to represent resources and HTTP methods are then used to perform actions on those
// resources
@Path("/v1/workers")
public class Worker {
  private final CuratorFramework curator;
  WorkerService worker;

  @Inject
  public Worker(CuratorFramework curator) {
    this.curator = curator;
    initWorker();
  }

  public void initWorker() {
    worker = new WorkerService(curator, ZKUtils.LEADER_ROOT);
  }

  @DELETE
  @Path("/{id}")
  public void stopWorker(@PathParam("id") String id) {
    worker.stop();
  }

  @GET
  @Path("/leader")
  public String getLeaderId() {
    return worker.getLeader().orElse("No leader found");
  }
}
