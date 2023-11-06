package com.saksham.mapreduce;

import com.saksham.mapreduce.serialization.JsonSerializer;
import com.saksham.mapreduce.serialization.Serializer;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;

public class FileReader  implements MRReader<String>{

    private BufferedReader br;
    public void init(String location, String taskId)
    {
        try {
            // TODO check if file can be read over the network
            br = new BufferedReader(new java.io.FileReader(location));
            //FileUtils.readDataFromFile(location,taskId)));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            throw  new RuntimeException("Exception reading data from the file " + location, e);
        }
    }

    @Override
    public String readTuple() throws IOException {
        String line;
        if ((line =  br.readLine()) != null)
        {
            return line;
        }
        return null;
    }
}
