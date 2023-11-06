package com.saksham.mapreduce.service;

import com.saksham.mapreduce.dao.CoordinatorDao;
import com.saksham.mapreduce.task.Task;
import com.saksham.mapreduce.types.PipelineStage;
import com.saksham.mapreduce.types.Status;
import com.saksham.mapreduce.types.TaskType;
import com.saksham.mapreduce.types.WorkItem;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.File;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.Thread.sleep;

@Slf4j
@Component
public class CoordinatorService {


    private CoordinatorDao coordinatorDao;

    private WorkerManagementService workerManagerService;

    private List<PipelineStage> pipelineStages;

    @Autowired
    public CoordinatorService(CoordinatorDao coordinatorDao,
                              WorkerManagementService workerManagerService) {
        this.coordinatorDao = coordinatorDao;
        this.workerManagerService = workerManagerService;
        this.pipelineStages = new ArrayList<>();
    }

    public void registerNewStage(PipelineStage pipelineStage) {
        pipelineStages.add(pipelineStage);
    }

    public void initializeNewStageScheduler(String startupPath) throws InterruptedException {
        String nextPath = startupPath;
        int stageIndex = 0;
        while (true) {
            if (stageIndex == pipelineStages.size()) {
                workerManagerService.shutDownWorkers();
                break;
            }
            // TODO put the stage name here
            if (coordinatorDao.isAllWorkFinished()) {
                // TODO get rid off type of stage.
                if (pipelineStages.get(stageIndex).getTaskType() == TaskType.MAPPER) {
                    initializeNextStage(nextPath, TaskType.MAPPER, pipelineStages.get(stageIndex),
                            workerManagerService.getMappers(), workerManagerService.getReducers());
                } else {
                    initializeNextStage(nextPath, TaskType.REDUCER, pipelineStages.get(stageIndex),
                            workerManagerService.getReducers(), 1);
                }
                nextPath = Paths.get(startupPath).getParent() + "/" + pipelineStages.get(stageIndex).getStageName();
                stageIndex++;
            }
            sleep(5000);
            workerManagerService.checkWorkerStatus();
        }
    }

    // Called When assignedWorkItems and InitializedWork Items are zero.
    private void initializeNextStage(String startupPath, TaskType taskType, PipelineStage pipelineStage,
                                     int totalWorkers, int outputPartitions) {
        ArrayList<String> allFilePaths = new ArrayList<>();
        coordinatorDao.clearInitializedItems();
        getAllFiles(startupPath, allFilePaths);
        Collections.sort(allFilePaths);
        // TODO partition according to changing parrallelism.
        if (taskType == TaskType.REDUCER) {
            ArrayList<String>[] files = new ArrayList[totalWorkers];
            for (int i = 0; i < allFilePaths.size(); i++) {
                if(files[i%totalWorkers] == null){
                    files[i%totalWorkers] = new ArrayList<>();
                }
                files[i % totalWorkers].add(allFilePaths.get(i));
            }
            for (int i =0; i< totalWorkers;i++){
                coordinatorDao.addNewWorkItem(new WorkItem()
                        .setFiles(files[i])
                        .setTimeout(10)
                        .setStatus(Status.INITIALIZED)
                        .setTaskType(taskType)
                        .setTaskId(UUID.randomUUID().toString())
                        .setLastStartTimestamp(System.currentTimeMillis())
                        .setVersion(new AtomicInteger(0))
                        .setStageName(pipelineStage.getStageName()).setTaskClassName(pipelineStage.getTaskClassName())
                        .setMrReaderClassName(pipelineStage.getMrReaderClassName())
                        .setOutputPartitions(outputPartitions));
            }
        }
        if (taskType == TaskType.MAPPER) {
            List<String> files = new ArrayList<>();
            for (int i = 0; i < allFilePaths.size(); i++) {
                // Create one task for each file to maximise parrallelism
                // TODO can be move to constructor.
                coordinatorDao.addNewWorkItem(new WorkItem()
                        .setFiles(Arrays.asList(allFilePaths.get(i)))
                        .setTimeout(10)
                        .setStatus(Status.INITIALIZED)
                        .setTaskType(taskType)
                        .setTaskId(UUID.randomUUID().toString())
                        .setLastStartTimestamp(System.currentTimeMillis())
                        .setVersion(new AtomicInteger(0))
                        .setStageName(pipelineStage.getStageName()).setTaskClassName(pipelineStage.getTaskClassName())
                        .setMrReaderClassName(pipelineStage.getMrReaderClassName())
                        .setOutputPartitions(outputPartitions));

            }
        }
        coordinatorDao.clearCompletedItems();
    }

    private void getAllFiles(String startupPath, ArrayList<String> allFilePaths) {
        File directory = new File(startupPath);
        String[] files = directory.list();
        for (String file : files) {

            String filePath = directory.getAbsolutePath() + "/" + file;
            if (new File(filePath).isDirectory()) {
                getAllFiles(filePath, allFilePaths);
                continue;
            }
            allFilePaths.add(filePath);
        }
    }

    public WorkItem getWorkItem(String workerIdentification) {
        return coordinatorDao.getWorkItem(workerIdentification);
    }



}
