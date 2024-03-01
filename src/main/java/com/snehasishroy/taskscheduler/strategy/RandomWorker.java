package com.snehasishroy.taskscheduler.strategy;

import java.util.List;
import org.apache.curator.framework.recipes.cache.ChildData;

public class RandomWorker implements WorkerPickerStrategy {
  @Override
  public ChildData evaluate(List<ChildData> workers) {
    int chosenWorker = (int) (Math.random() * workers.size());
    return workers.get(chosenWorker);
  }
}
