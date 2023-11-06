package com.saksham.mapreduce;

import com.saksham.mapreduce.serialization.KeyValuePairSerializer;
import com.saksham.mapreduce.serialization.Serializer;
import com.saksham.mapreduce.types.KeyValuePair;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;

@Slf4j
public class KeyValueReader implements MRReader<KeyValuePair<String, Integer>>{

    private BufferedReader br;

    private final Serializer<KeyValuePair<String, Integer>,String > serializer = new KeyValuePairSerializer<>();
    @Override
    public KeyValuePair<String,Integer> readTuple() throws IOException {
        String line;
        if ((line =  br.readLine()) != null)
        {
            // Assume each line is a key value pair.
            log.info("LINE IS [{}]", line);
            return serializer.deserialize(line);
        }
        return null;
    }

    @Override
    public void init(String location, String taskId) {
        try {
            // TODO check if we can read file from a location
            // br = new BufferedReader(new java.io.FileReader(FileUtils.readDataFromFile(location,taskId)));
            br = new BufferedReader(new java.io.FileReader(location));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            throw  new RuntimeException("Exception reading data from the file " + location);
        }
    }
}
