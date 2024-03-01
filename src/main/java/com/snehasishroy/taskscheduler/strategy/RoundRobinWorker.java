package com.snehasishroy.taskscheduler.strategy;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.curator.framework.recipes.cache.ChildData;

public class RoundRobinWorker implements WorkerPickerStrategy {
    AtomicInteger index = new AtomicInteger(0); // atomic because this will be accessed from multiple threads

    @Override
    public ChildData evaluate(List<ChildData> workers) {
        int chosenIndex;
        while (true) { // repeat this until compare and set operation is succeeded
            chosenIndex = index.get();
            int nextIndex = (chosenIndex + 1) < workers.size() ? (chosenIndex + 1) : 0;
            if (index.compareAndSet(chosenIndex, nextIndex)) {
                break;
            }
        }
        return workers.get(chosenIndex);
    }
}
