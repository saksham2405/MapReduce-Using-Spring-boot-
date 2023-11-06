package com.saksham.mapreduce.task;

import com.saksham.mapreduce.types.KeyValuePair;

import java.io.FileOutputStream;
import java.util.List;

public interface Mapper<INPUT> extends Task<INPUT, KeyValuePair<?, ? >>{

    public KeyValuePair<?, ?> map(INPUT input);
    // TODO Exclude these functions from user inherited map/reduce task
}
