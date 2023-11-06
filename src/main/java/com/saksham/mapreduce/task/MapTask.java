package com.saksham.mapreduce.task;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.saksham.mapreduce.MRReader;
import com.saksham.mapreduce.types.KeyValuePair;
import com.saksham.mapreduce.types.WorkItem;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.DigestUtils;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public abstract class MapTask<INPUT> implements Mapper<INPUT> {

    private String taskId;

    private int reducePartitions;

    // Over network filesystem.
    private MRReader<INPUT> mrReader;

    private void init(String taskId, int reducePartitions, MRReader mrReader) {
        this.taskId = taskId;
        this.reducePartitions = reducePartitions;
        this.mrReader = mrReader;
    }
    private void initReader(String filePath)
    {
        this.mrReader.init(filePath, taskId);
    }

    @Override
    public void process(WorkItem workItem) throws IOException {
        /**
         * Curl Call for reading data.
         * Pass Data to user Code
         * Write Data to output files
         */
        INPUT line;
        try {
            init(workItem.getTaskId(), workItem.getOutputPartitions(),
                    (MRReader<INPUT>) Class.forName(workItem.getMrReaderClassName()).newInstance());
        } catch (Exception e) {
            e.printStackTrace();
        }
        List<FileOutputStream> reducer_files = new ArrayList<>();
        List<String> files = new ArrayList<>();
        Path path = Paths.get(workItem.getFiles().get(0));
        String finalDumpPath = path.getParent().getParent() + "/" + workItem.getStageName() + "/" + workItem.getTaskId();
        log.info("Final Dump path [{}] for workItem [{}], reducePartitions [{}]", finalDumpPath, workItem,reducePartitions);
        for (int i = 0; i < reducePartitions; i++) {
            File file = new File(finalDumpPath + "/" + i);
            file.getParentFile().mkdirs();
            file.createNewFile();
            reducer_files.add(new FileOutputStream(file));
            files.add(file.getCanonicalPath());
        }
        log.info("Reducer Files [{}]", files);
        int lineNumber = 0;
        for(String filePath: workItem.getFiles()) {
            try {
                // TODO try with multiple partitions.
                initReader(filePath);
            } catch (Exception e) {
                e.printStackTrace();
            }
            // Here the PCoders will come in to convert the file contents to objects.
            while ((line = mrReader.readTuple()) != null) {
                KeyValuePair<?, ?> output = map(line);
                // Improve the hashing
                if (output != null) {
                    FileOutputStream fos = reducer_files.get((output.getKey().hashCode() & Integer.MAX_VALUE)  % reducePartitions);
                    fos.write(new ObjectMapper().writeValueAsString(output).getBytes());
                    fos.write(System.getProperty("line.separator").getBytes());
                } else {
                    throw new NullPointerException("Return value of map function should not be null, I/P to the function " + line);
                }
            }
        }

    }


}
