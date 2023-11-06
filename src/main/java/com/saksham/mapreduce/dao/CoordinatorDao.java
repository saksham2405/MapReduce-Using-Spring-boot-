package com.saksham.mapreduce.dao;

import com.saksham.mapreduce.types.Status;
import com.saksham.mapreduce.types.TaskType;
import com.saksham.mapreduce.types.WorkItem;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Component
@Slf4j
public class CoordinatorDao {

    private Map<String,WorkItem> initializedItems;

    // At any given time, completed work items would only contain work items pertaining to a specific stage.
    private HashSet<WorkItem> completedWorkItems;

    private Map<String, WorkItem> assignedWorkItems;

    private Map<String, List<String>> workerToTasksMap;

    private final WorkItem emptyWorkItem;


    @Autowired
    public CoordinatorDao() {
        initializedItems = new ConcurrentHashMap<>();
        assignedWorkItems = new ConcurrentHashMap<>();
        completedWorkItems = new HashSet<>();
        emptyWorkItem = new WorkItem().setTaskId("Empty");
        workerToTasksMap = new HashMap<>();
    }


    public void updateWorkItemStatus(Map<String, WorkItem> taskIdStatusMap) {
        for (Map.Entry<String, WorkItem> entry : taskIdStatusMap.entrySet()) {
            if (entry.getValue().getStatus() == Status.FINISHED) {
                if(assignedWorkItems.containsKey(entry.getKey())) {
                    log.info("Marking WorkItem [{}] as finished", entry.getValue());
                    completedWorkItems.add(assignedWorkItems.get(entry.getKey()));
                    // Single threaded, safely can be removed.
                    assignedWorkItems.remove(entry.getKey());
                }
                else{
                    log.error("Ignoring WorkItem [{}] ", entry.getValue());
                }

            }
            if (entry.getValue().getStatus() == Status.FAILED) {
                log.info("Marking WorkItem [{}] as failed", entry.getValue());
                WorkItem workItem = assignedWorkItems.get(entry.getKey());
                assignedWorkItems.remove(entry.getKey());
                // can have retries check here only.
                if (!workItem.equals(emptyWorkItem)) {
                    initializedItems.get(workItem.getTaskId()).getVersion().set(0);
                }
            }
        }
    }

    public void reinitalizeAssignedTasks(String workerId){
        for(String taskId: workerToTasksMap.get(workerId)){
            WorkItem workItem = assignedWorkItems.getOrDefault(taskId,emptyWorkItem);
            if(workItem!=emptyWorkItem){
                assignedWorkItems.remove(workItem.getTaskId());
                initializedItems.get(workItem.getTaskId()).getVersion().set(0);
            }
        }
    }

    public WorkItem getWorkItem(String workerIdentfication) {
        for (Map.Entry<String,WorkItem> entry : initializedItems.entrySet()) {
            WorkItem workItem = entry.getValue();
            if (workItem.getVersion().compareAndSet(0, 1)) {
                assignedWorkItems.put(workItem.getTaskId(), workItem);
                if(workerToTasksMap.containsKey(workerIdentfication)){
                    workerToTasksMap.get(workerIdentfication).add(workItem.getTaskId());
                }
                else{
                    ArrayList<String> workItems = new ArrayList<>();
                    workItems.add(workItem.getTaskId());
                    workerToTasksMap.put(workerIdentfication, workItems);
                }
                return workItem;
            }
        }
        // TODO can throw exception here.
        return null;
    }

    public void addNewWorkItem(WorkItem workItem) {
        initializedItems.put(workItem.getTaskId(), workItem);
    }

    public boolean isAllWorkFinished() {
        return assignedWorkItems.size() == 0 && initializedItems.size() == completedWorkItems.size();
    }

    public TaskType getCurrentTaskType() {
        return completedWorkItems.iterator().next().getTaskType();
    }

    public void clearCompletedItems() {
        completedWorkItems.clear();
    }

    public void clearInitializedItems() {
        initializedItems.clear();
    }



}
