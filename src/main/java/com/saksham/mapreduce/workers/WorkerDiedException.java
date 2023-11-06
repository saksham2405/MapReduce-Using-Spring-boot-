package com.saksham.mapreduce.workers;

public class WorkerDiedException  extends  Exception{

    private String message;

    public WorkerDiedException(String message) {
        this.message = message;
    }
}
