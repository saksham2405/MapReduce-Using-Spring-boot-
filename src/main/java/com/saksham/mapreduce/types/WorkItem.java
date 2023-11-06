package com.saksham.mapreduce.types;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.experimental.Accessors;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Data
@Accessors(chain = true)
public class WorkItem implements Serializable {

    private String taskId;

    private int timeout;

    private List<String> files;

    private TaskType taskType;

    private Status status;

    private long lastStartTimestamp;

    private AtomicInteger version;

    private String taskClassName;

    private String mrReaderClassName;

    private String stageName;

    private int outputPartitions;

    private int lastUpdateTime;
}
