package com.saksham.mapreduce.types;

import com.saksham.mapreduce.MRReader;
import lombok.Data;
import lombok.experimental.Accessors;

import javax.print.DocFlavor;

@Data
@Accessors(chain = true)
public class PipelineStage {

    String stageName;

    String mrReaderClassName;

    String taskClassName;

    TaskType taskType;
}
