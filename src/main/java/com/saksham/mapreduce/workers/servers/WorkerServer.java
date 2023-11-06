package com.saksham.mapreduce.workers.servers;

import com.saksham.mapreduce.workers.dao.WorkerDao;
import com.saksham.mapreduce.types.Status;
import com.saksham.mapreduce.types.WorkItem;
import com.saksham.mapreduce.workers.ManagerClient;
import com.saksham.mapreduce.workers.Worker;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Controller
@Service
@Slf4j
public class WorkerServer {

    private final WorkerDao workerDao;

    private final Worker worker;

    // TODO correct this for proper workers.
    @Autowired
    public WorkerServer(WorkerDao workerDao, ManagerClient managerClient) {
        this.workerDao = workerDao;
        worker = new Worker(workerDao, managerClient, "");
    }

    //TODO please exit command.
    @RequestMapping("/ping")
    public ResponseEntity<Map<String, WorkItem>> ping() {
        log.info("StatusMap [{}] and workerDao [{}]", workerDao.getTaskStatusMap(), workerDao.hashCode());
        return ResponseEntity.status(200).body(workerDao.getTaskStatusMap());
    }

    @RequestMapping("/taskStatus/{taskId}")
    public ResponseEntity<Status> getTaskStatus(@PathVariable String taskId) {
        if (workerDao.getTaskStatusMap().containsKey(taskId)) {
            return ResponseEntity.status(200).body(workerDao.getTaskStatusMap().get(taskId).getStatus());
        } else {
            return ResponseEntity.status(404).body(Status.NO_SUCH_TASK);
        }
    }

    @RequestMapping("/exit")
    public ResponseEntity<String> exit() {
        worker.kill();
        return ResponseEntity.status(200).body("Killing the worker in next 5s");
    }

    // TODO last ping status exit.
}
