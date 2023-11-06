package com.saksham.mapreduce.servers;

import com.saksham.mapreduce.service.CoordinatorService;
import com.saksham.mapreduce.types.WorkItem;
import jakarta.servlet.http.HttpServletRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

@Controller
@Service
@Slf4j
public class CoordinatorServer {

    @Autowired
    private CoordinatorService coordinatorService;

    // TODO PUT workerId, and check whether the worker is presumed dead.
    @RequestMapping(path =  "/manager/task", method = RequestMethod.GET)
    public ResponseEntity<WorkItem> getTask(@RequestParam("worker")String workerId, HttpServletRequest request){
        WorkItem workItem = coordinatorService.getWorkItem(workerId);
        if(workItem != null)
        {
            log.info("Responding with workItem [{}] to worker[{}]" ,workItem, workerId);
            return ResponseEntity.status(200).body(workItem);
        }
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(null);
    }
}
