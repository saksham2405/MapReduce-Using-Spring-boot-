package com.saksham.mapreduce.coordinator;

import java.io.File;
import java.io.IOException;

public class CoordinatorUtils {

    public static String startupWorker(String machineIp, int port, String jarFile) throws IOException {

        String command[]  = {"java", "-cp", jarFile,  "-Dserver.port=" + port,
                "-Dloader.main=com.saksham.mapreduce.workers.WorkerMain",
                "org.springframework.boot.loader.PropertiesLauncher"
        };
        ProcessBuilder worker = new ProcessBuilder(command);
        /*ProcessBuilder worker =
                new ProcessBuilder("java -cp  -Dserver.port=" + port
                        + " " + jarFile + " com.saksham.mapreduce.WorkerMain ");*/
        String workerURI = "http://"+machineIp+":" + port;
        worker.directory(new File("/Users/sakshamagrawal/Desktop/workers"));
        File log = new File("/Users/sakshamagrawal/Desktop/workers/worker-" + port + ".log");
        if(!log.exists()) {
            log.createNewFile();
        }
        worker.redirectErrorStream(true);
        worker.redirectOutput(ProcessBuilder.Redirect.appendTo(log));
        Process p = worker.start();
        assert worker.redirectInput() == ProcessBuilder.Redirect.PIPE;
        assert worker.redirectOutput().file() == log;
        assert p.getInputStream().read() == -1;
        return workerURI;
    }
}
