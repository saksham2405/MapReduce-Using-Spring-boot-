package com.saksham.mapreduce.workers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.saksham.mapreduce.NoTaskRecievedException;
import com.saksham.mapreduce.types.Status;
import com.saksham.mapreduce.types.WorkItem;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.List;

@Slf4j
@Component
public class ManagerClient {

    // TODO add connection pooling.
   /* String hostname;
    String port;*/

    // TODO remove this
    /*@Autowired
    public ManagerClient(@Value("${mr.manager.hostname}") String hostname,
                         @Value("${mr.workers}") String port) {
        System.out.println("Manager details " + hostname + " "  + port);
        this.hostname = hostname;
        this.port = port;
    }*/

    /*public void notifyFileLocationsToManager(List<String> fileLocations,
                                             String taskId, String stageName) throws JsonProcessingException {
        // TODO update this path
        String managerUrlPath = "";
        HttpStatus status =
                new RestTemplate().execute(new ObjectMapper().writeValueAsString(fileLocations),
                        HttpMethod.GET, null, ClientHttpResponse::getStatusCode);
        assert status != null;
        if (status.is2xxSuccessful()) {
            log.info("Successfully forwarded the filelocations {} from taskId {} with taskName{}",
                    fileLocations, taskId, stageName);
        }
    }

    public void notifyReducerFileLocationToManager(String fileLocation, String taskId, String stageName) throws JsonProcessingException {
        String managerUrlPath = "";
        HttpStatus status =
                new RestTemplate().execute(new ObjectMapper().writeValueAsString(fileLocation),
                        HttpMethod.GET, null, ClientHttpResponse::getStatusCode);
        assert status != null;
        if (status.is2xxSuccessful()) {
            log.info("Successfully forwarded the filelocation {} from taskId {} with taskName{}",
                    fileLocation, taskId, stageName);
        }
    }*/

    @Value("${server.port}")
    private int port;

    public WorkItem getTaskFromManager(String workerName) throws NoTaskRecievedException {
        String workerId = "http://127.0.0.1:"+port;
        String managerUrlPath = "http://127.0.0.1:8080/manager/task?worker="+workerId;

        try {
            log.info("CAALLING MANAGER WITH URL [{}]", managerUrlPath);
            ResponseEntity<WorkItem> response = new RestTemplate().getForEntity(managerUrlPath, WorkItem.class);
            log.info("RESPONSE FROM MANAGER [{}]", response);
            if (response.getStatusCode().is2xxSuccessful()) {
                WorkItem workItem = response.getBody();
                log.info("Successfully recieved WorkItem[{}] for Worker[{}]",
                        workItem, workerName);
                return workItem;
            } else {
                log.info("failed to get task for WORKER [{}] ", workerName);
                throw new NoTaskRecievedException("NO task from manager for worker " + workerName);
            }
        }
        catch (Exception e)
        {
            // log.info("failed to get task for WORKER [{}] ", workerName, e);
            throw new NoTaskRecievedException("NO task from manager for worker " + workerName);
            // throw  new RuntimeException("Unbale to contact manager");
        }
    }

    /*public void notifyTaskStatusToManager(Status status, String taskId, String stageName) {
        String managerUrlPath = "";
        try {
            HttpStatus requestStatus = new RestTemplate().execute(new ObjectMapper().writeValueAsString(status),
                    HttpMethod.GET, null, ClientHttpResponse::getStatusCode);
            assert requestStatus != null;
            if (requestStatus.is2xxSuccessful()) {
                log.info("Successfully notified status {} to manager for task with Id {} and name {}",
                        status, taskId, stageName);
            }
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            throw new RuntimeException("Failed Notifying progress to manager from task Id "
                    +taskId+" and stage "+stageName);

        }

    }*/
}
