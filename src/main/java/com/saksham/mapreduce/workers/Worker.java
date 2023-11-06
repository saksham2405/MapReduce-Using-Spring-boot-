package com.saksham.mapreduce.workers;

import com.saksham.mapreduce.workers.dao.WorkerDao;
import com.saksham.mapreduce.NoTaskRecievedException;
import com.saksham.mapreduce.task.Task;
import com.saksham.mapreduce.types.WorkItem;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class Worker {

    // TODO change this to be configurable
    private ExecutorService executorService;

    private WorkerDao workerDao;

    private Queue<Boolean> fetchQueue;

    public static final int THREADS_PER_WORKER = 2;

    private String name;

    private ManagerClient managerClient;

    private boolean isKillIssued;
    public Worker(WorkerDao workerDao, ManagerClient managerClient, String name) {
        // TODO make this configurable
        executorService = Executors.newFixedThreadPool(THREADS_PER_WORKER);
        this.workerDao = workerDao;
        fetchQueue = new LinkedList<>();
        // TODO check to pass the name
        this.name = name;
        this.managerClient = managerClient;
        this.isKillIssued = false;
        initilizeTaskScheduler();
    }

    private void initilizeTaskScheduler()  {
        fetchQueue.add(Boolean.TRUE);
        Thread requestorThread = new Thread(() -> {
            int consecutiveFailures = 0;
            boolean lastStatus = true;
            // Sleeping this thread to let the server start.
            try {
                Thread.sleep(5000L);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            while (fetchQueue.peek() != null) {
                if(isKillIssued){
                    System.exit(0);
                }
                try {
                    Thread.sleep(5000L);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                log.info("FETCH QUEUE [{}] SIZE [{}]" , fetchQueue.peek(), fetchQueue.size() );
                if (fetchQueue.peek() == lastStatus && !lastStatus) {
                    consecutiveFailures++;
                } else {
                    lastStatus = fetchQueue.peek();
                    consecutiveFailures = 0;
                }
                // TODO check this
                /*if (consecutiveFailures == 4) {
                    log.error("Exiting the worker as there are too many failures ");
                    System.exit(0);
                }*/
                fetchQueue.remove();
                try {
                    WorkItem workItem = managerClient.getTaskFromManager(name);
                    log.info("GOT task [{}] from manager", workItem);
                    workerDao.addWorkItem(workItem);
                    // IMPL FOR ACTUAL CODE CALL.
                    executorService.submit(() -> {
                        try {
                            workerDao.markInProgress(workItem);
                            // Actual code call
                            ((Task)Class.forName(workItem.getTaskClassName()).newInstance()).process(workItem);
                            log.info("Marking task [{}] as success and workerDao is [{}]", workItem, workerDao.hashCode());
                            workerDao.markSuccess(workItem);
                            log.info("Current PID is [{}]", ProcessHandle.current().pid());
                            fetchQueue.add(Boolean.TRUE);
                        } catch (IOException e) {
                            log.error("Unable to process the task with taskId [{}] and stage [{}]", workItem.getTaskId(),
                                    workItem.getStatus(), e);
                            fetchQueue.add(Boolean.FALSE);
                        } catch (Exception e) {
                            log.error("Processing for workItem [{}] failed", workItem, e);
                            fetchQueue.add(Boolean.FALSE);
                        }
                    }).get();
                } catch (NoTaskRecievedException e) {
                    // Failed attempts to get the tasks.
                    fetchQueue.add(false);
                } catch (ExecutionException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }
        }, "Worker_" + name + "_background_thread");
        requestorThread.start();
    }
    public void kill(){
        isKillIssued = true;
    }
}
