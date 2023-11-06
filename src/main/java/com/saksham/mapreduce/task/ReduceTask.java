package com.saksham.mapreduce.task;

import com.saksham.mapreduce.MRReader;
import com.saksham.mapreduce.serialization.KeyValuePairSerializer;
import com.saksham.mapreduce.serialization.Serializer;
import com.saksham.mapreduce.types.KeyValuePair;
import com.saksham.mapreduce.types.WorkItem;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public abstract class ReduceTask<KEY, VALUE, OUTPUT_KEY, OUTPUT_VALUE> implements Reducer<KEY, VALUE, OUTPUT_KEY, OUTPUT_VALUE> {

    public MRReader<KeyValuePair<KEY, VALUE>> keyValueReader;
    public String taskId;
    public Serializer<KeyValuePair<OUTPUT_KEY, OUTPUT_VALUE>, String> serializer;
    public String stageName;

    public void init(MRReader<KeyValuePair<KEY, VALUE>> keyValueReader,
                     String taskId,
                     Serializer<KeyValuePair<OUTPUT_KEY, OUTPUT_VALUE>, String> serializer,
                     String stageName, String filePath) {
        this.keyValueReader = keyValueReader;
        this.taskId = taskId;
        this.serializer = serializer;
        this.stageName = stageName;
        this.keyValueReader.init(filePath, taskId);
    }

    @Override
    public void process(WorkItem workItem) throws FileNotFoundException {
        /**
         * Read data from intermediate file
         * Pass Data to user Code
         * Write Data to output files
         */
        Map<KEY, List<VALUE>> keyGroups = new HashMap<>();
        KeyValuePair<KEY, VALUE> keyValuePair;

        for(String file: workItem.getFiles()) {
            try {
                init((MRReader<KeyValuePair<KEY, VALUE>>) Class.forName(workItem.getMrReaderClassName()).newInstance(),
                        workItem.getTaskId(), new KeyValuePairSerializer<OUTPUT_KEY, OUTPUT_VALUE>(), workItem.getStageName(),
                        file);
            } catch (Exception e) {
                e.printStackTrace();
            }
            while (true) {
                try {
                    if ((keyValuePair = keyValueReader.readTuple()) == null) {
                        break;
                    }
                    if (!keyGroups.containsKey(keyValuePair.getKey())) {
                        keyGroups.put(keyValuePair.getKey(), new ArrayList<>());
                        keyGroups.get(keyValuePair.getKey()).add(keyValuePair.getValue());
                    } else {
                        keyGroups.get(keyValuePair.getKey()).add(keyValuePair.getValue());
                    }

                } catch (IOException e) {
                    e.printStackTrace();
                }

            }
        }

        List<KeyValuePair<OUTPUT_KEY, OUTPUT_VALUE>> outputs = new ArrayList<>();
        // writing to file which would be read later.
        String finalDumpPath = Paths.get(workItem.getFiles().get(0)).getParent() + "/" + workItem.getStageName();
        log.info("Reduce Final Dump path [{}] for workItem [{}]", finalDumpPath, workItem);
        File file = new File(finalDumpPath + "/" + taskId);
        file.getParentFile().mkdirs();
        try {
            file.createNewFile();
        } catch (IOException e) {
            // TODO handle this properly.
            e.printStackTrace();
        }
        FileOutputStream fileOutputStream = new FileOutputStream(file);
        for (Map.Entry<KEY, List<VALUE>> entry : keyGroups.entrySet()) {
            KeyValuePair<KEY, List<VALUE>> inp = new KeyValuePair<>();
            inp.setKey(entry.getKey()).setValue(entry.getValue());
            log.info("Entry is [{}]", entry);
            KeyValuePair<OUTPUT_KEY, OUTPUT_VALUE> output = reduce(inp);

            try {
                fileOutputStream.write(serializer.serialize(output).getBytes());
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException("Cannot write to File");
            }
        }

    }
}
