package com.saksham.mapreduce;

import com.saksham.mapreduce.serialization.Serializer;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;

public interface MRReader<T> extends Serializable {

    public T readTuple() throws IOException;

    public void init(String location, String taskId);
}
