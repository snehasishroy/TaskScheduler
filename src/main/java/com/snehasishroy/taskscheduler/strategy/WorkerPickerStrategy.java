package com.snehasishroy.taskscheduler.strategy;

import org.apache.curator.framework.recipes.cache.ChildData;

import java.util.List;

public interface WorkerPickerStrategy {
    ChildData evaluate(List<ChildData> workers);
}
