package com.saksham.mapreduce.task;

import com.saksham.mapreduce.MRReader;
import com.saksham.mapreduce.types.WorkItem;

import java.io.IOException;
import java.io.Serializable;

public interface Task<INPUT, OUTPUT> extends Serializable {

     void process(WorkItem workItem) throws IOException;
}
