package com.snehasishroy.taskscheduler.strategy;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.zookeeper.data.Stat;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class RoundRobinWorkerTest {

  List<ChildData> workers;

  @BeforeEach
  public void setup() {
    ChildData child1 = new ChildData("/a", new Stat(), new byte[0]);
    ChildData child2 = new ChildData("/b", new Stat(), new byte[0]);
    ChildData child3 = new ChildData("/c", new Stat(), new byte[0]);
    ChildData child4 = new ChildData("/d", new Stat(), new byte[0]);
    ChildData child5 = new ChildData("/e", new Stat(), new byte[0]);
    workers = List.of(child1, child2, child3, child4, child5);
  }

  @Test
  public void testEvaluateSerially() {
    RoundRobinWorker roundRobinWorker = new RoundRobinWorker();
    for (int i = 0; i < 6; i++) {
      ChildData res = roundRobinWorker.evaluate(workers);
      Assertions.assertEquals(workers.get(i % workers.size()), res);
    }
  }

  @Test
  public void testEvaluateConcurrently() throws ExecutionException, InterruptedException {
    RoundRobinWorker roundRobinWorker = new RoundRobinWorker();
    ExecutorService executorService = Executors.newFixedThreadPool(10);
    List<CompletableFuture<ChildData>> futures = new ArrayList<>();
    for (int i = 0; i < 6; i++) {
      CompletableFuture<ChildData> future = CompletableFuture.supplyAsync(() -> roundRobinWorker.evaluate(workers), executorService);
      futures.add(future);
    }
    CompletableFuture<List<ChildData>> aggregate = CompletableFuture.completedFuture(new ArrayList<>());
    for (CompletableFuture<ChildData> future : futures) {
      aggregate = aggregate.thenCompose(list -> {
          try {
              list.add(future.get());
          } catch (InterruptedException | ExecutionException e) {
              throw new RuntimeException(e);
          }
          return CompletableFuture.completedFuture(list);
      });
    }
    List<ChildData> results = aggregate.join();
    for(int i = 0; i < 6; i++) {
      Assertions.assertEquals(workers.get(i % workers.size()), results.get(i));
    }
  }
}
