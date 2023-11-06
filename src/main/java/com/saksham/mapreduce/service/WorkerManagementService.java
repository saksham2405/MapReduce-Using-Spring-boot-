package com.saksham.mapreduce.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.saksham.mapreduce.coordinator.CoordinatorUtils;
import com.saksham.mapreduce.dao.CoordinatorDao;
import com.saksham.mapreduce.types.Status;
import com.saksham.mapreduce.types.WorkItem;
import com.saksham.mapreduce.workers.WorkerDiedException;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Component
@Slf4j
public class WorkerManagementService {

    // Ping and remove workers who die.
    private HashSet<String> current_workers;

    private HashMap<String, Integer> dormant_workers;

    private String[] workerMachines;

    private final CoordinatorDao coordinatorDao;

    private final Random randomGen = new Random();

    private final String jarFile;

    // TODO increase/decrease if worker dies.
    @Getter
    private int totalWorkers;

    @Getter
    private int mappers;

    @Getter
    private int reducers;

    @Autowired
    private WorkerManagementService(@Value("${mr.workers}") Integer totalWorkers, CoordinatorDao coordinatorDao,
                                    @Value("${mr.jar.path}") String jarFile,
                                    @Value("${mr.workermachines.ip}") String machines,
                                    @Value("${mr.mappers}") Integer mappers,
                                    @Value("${mr.reducers}") Integer reducers){

        this.workerMachines = machines.split(",");
        this.jarFile = jarFile;
        this.current_workers = new HashSet<>();
        this.dormant_workers = new HashMap<>();
        this.coordinatorDao = coordinatorDao;
        this.totalWorkers = totalWorkers;
        this.mappers = mappers;
        this.reducers = reducers;
    }

    public void initWorkers(){
        for(int i=0;i<totalWorkers;i++) {
            initializeNewWorker();
        }
        // Every worker startup takes 5s to complete.
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        // initializeWorkerStatusScheduler();
    }

    private void initializeNewWorker(){
        // RUN java -jar for the same jar here.
        int randomNumber = randomGen.nextInt()%1000 + 8090;
        int randomIndex = randomGen.nextInt()%workerMachines.length;
        try {
            current_workers.add(CoordinatorUtils.startupWorker(workerMachines[randomIndex],randomNumber, jarFile));
        } catch (IOException e) {
            log.error("Error in starting up a worker with port [{}]", randomNumber);
        }
    }
    private void initializeWorkerStatusScheduler() {
        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        // Ping workers.
        try {
            // TODO change this to handle failure/startup properly.
            executorService.schedule(this::checkWorkerStatus, 2, TimeUnit.SECONDS);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public void shutDownWorkers(){
        for(String worker: current_workers){
            log.info("Killing worker [{}]", worker);
            try{
                ResponseEntity<String> response = new RestTemplate().getForEntity(worker+"/exit", String.class);
                if(response.getStatusCode().is2xxSuccessful()){
                    log.info("Killed worker [{}]", worker);
                }
            }
            catch (Exception e){
                e.printStackTrace();
                log.info("No response from worker [{}]", worker);
            }
        }
    }
    public void checkWorkerStatus() {
        for (String worker : current_workers) {
            //Ping a process
            log.info("Pinging worker [{}]", worker);
            try {
                ResponseEntity<String> response = new RestTemplate()
                        .getForEntity(worker + "/ping", String.class);
                // log.info("Response from worker [{}]", response);
                if (response.getStatusCode().is2xxSuccessful()) {
                    try {
                        // Returning workItem Instead.
                        HashMap<String, WorkItem> workItemStatusMap = new ObjectMapper()
                                .readValue(response.getBody(), new TypeReference<HashMap<String, WorkItem>>() {
                                });
                        // log.info("Status from the worker with id {} {}", worker, workItemStatusMap);
                        coordinatorDao.updateWorkItemStatus(workItemStatusMap);
                    } catch (JsonProcessingException e) {
                        log.error("Error processing response from worker [{}]", worker, e);
                    }
                }
            }
            catch (Exception e)
            {
                log.info("No response from the worker with id {}", worker);
                if(dormant_workers.getOrDefault(worker,0) > 0) {
                    log.debug("Assuming worker [{}] is dead now", worker);
                    current_workers.remove(worker);
                    dormant_workers.remove(worker);
                    coordinatorDao.reinitalizeAssignedTasks(worker);
                    initializeNewWorker();
                }
                else{
                    log.info("Moving worker [{}] to dormant worker", worker);
                    dormant_workers.put(worker, dormant_workers.getOrDefault(worker,0) + 1);
                }
            }
        }
    }
}
