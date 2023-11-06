package com.saksham.example.WordCount;

import com.saksham.mapreduce.FileReader;
import com.saksham.mapreduce.KeyValueReader;
import com.saksham.mapreduce.MapReduceJob;
import com.saksham.mapreduce.types.KeyValuePair;
import com.saksham.mapreduce.types.TaskType;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;

import java.io.File;

@SpringBootApplication
@ComponentScan(value = "com.saksham", excludeFilters = @ComponentScan.Filter(type = FilterType.REGEX,
        pattern = "com.saksham.mapreduce.workers.*"))
public class WordCountMain {

    public static void main(String[] args){
        ConfigurableApplicationContext context = SpringApplication.run(WordCountMain.class, args);
        MapReduceJob mapReduceJob = context.getBean(MapReduceJob.class);
        // TODO there should be no need to pass taskType
        mapReduceJob.registerStage("EmitWordCount", FileReader.class.getCanonicalName(),
                SentenceMapper.class.getCanonicalName(), TaskType.MAPPER );
        mapReduceJob.registerStage("ReduceWordCount", KeyValueReader.class.getCanonicalName(),
                WordCountReducer.class.getCanonicalName(), TaskType.REDUCER );
        System.out.println("Parent PROCESS PID IS " + ProcessHandle.current().pid());
        try {
            mapReduceJob.runJob(context);
        }
        catch (Exception e){
            e.printStackTrace();
        }
        System.out.println("completed Successfully");
    }
}
