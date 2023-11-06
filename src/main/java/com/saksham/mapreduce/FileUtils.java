package com.saksham.mapreduce;

import org.springframework.http.HttpMethod;
import org.springframework.util.StreamUtils;
import org.springframework.web.client.RestTemplate;

import java.io.File;
import java.io.FileOutputStream;

public class FileUtils {
    public static File readDataFromFile(String fileLocation, String taskId)
    {
        // TODO check if we can stream this data.
        return new RestTemplate().execute(fileLocation, HttpMethod.GET, null, clientHttpResponse -> {
            File file = File.createTempFile("download", taskId);
            StreamUtils.copy(clientHttpResponse.getBody(), new FileOutputStream(file));
            return file;
        });
    }
}
