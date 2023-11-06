package com.saksham.mapreduce;

import com.saksham.mapreduce.service.CoordinatorService;
import com.saksham.mapreduce.service.WorkerManagementService;
import com.saksham.mapreduce.types.PipelineStage;
import com.saksham.mapreduce.types.TaskType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.stereotype.Component;

@Component
public class MapReduceJob {

    @Autowired
    private CoordinatorService coordinatorService;

    @Autowired
    private WorkerManagementService workerManagementService;

    @Value("${mr.startup.path}")
    private String startupPath;

    // TODO extend to previous stage.
    public void registerStage(String stageName, String mrReaderClassName, String taskClassName,
                              TaskType taskType) {

        coordinatorService.registerNewStage(new PipelineStage()
                .setMrReaderClassName(mrReaderClassName)
                .setStageName(stageName)
                .setTaskClassName(taskClassName)
                .setTaskType(taskType));
    }

    public void runJob(ConfigurableApplicationContext context) throws InterruptedException {
        workerManagementService.initWorkers();
        coordinatorService.initializeNewStageScheduler(startupPath);
        SpringApplication.exit(context,()->0);

    }
}
