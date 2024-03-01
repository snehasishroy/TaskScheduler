package com.snehasishroy.taskscheduler.strategy;

import java.util.List;
import org.apache.curator.framework.recipes.cache.ChildData;

public interface WorkerPickerStrategy {
    ChildData evaluate(List<ChildData> workers);
}
