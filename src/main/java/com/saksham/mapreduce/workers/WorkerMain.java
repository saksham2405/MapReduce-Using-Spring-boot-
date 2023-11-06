package com.saksham.mapreduce.workers;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

import java.io.File;
import java.util.ArrayList;

@SpringBootApplication
@ComponentScan(basePackages = "com.saksham.mapreduce.workers")
public class WorkerMain {
    public static void main(String[] args) {

        SpringApplication.run(WorkerMain.class, args);
    }
}
