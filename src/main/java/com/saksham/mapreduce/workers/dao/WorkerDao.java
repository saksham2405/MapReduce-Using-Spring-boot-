package com.saksham.mapreduce.workers.dao;

import com.saksham.mapreduce.types.Status;
import com.saksham.mapreduce.types.WorkItem;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
public class WorkerDao {

    private final Map<String, WorkItem> taskStatusMap;
    public WorkerDao()
    {
        taskStatusMap = new HashMap<>();
    }

    public Map<String, WorkItem> getTaskStatusMap(){
        return taskStatusMap;
    }

    public void addWorkItem(WorkItem workItem){
        // TODO check taskId is populated in manager.
        taskStatusMap.put(workItem.getTaskId(), workItem);
    }

    public void markSuccess(WorkItem workItem){
        taskStatusMap.get(workItem.getTaskId()).setStatus(Status.FINISHED);
    }
    public void markInProgress(WorkItem workItem){
        taskStatusMap.get(workItem.getTaskId()).setStatus(Status.IN_PROGRESS);
    }

}
