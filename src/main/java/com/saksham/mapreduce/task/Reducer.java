package com.saksham.mapreduce.task;

import com.saksham.mapreduce.types.KeyValuePair;

import java.util.List;

public interface Reducer<KEY, VALUE, OUTPUT_KEY, OUTPUT_VALUE> extends Task<KeyValuePair<KEY, VALUE>, KeyValuePair<OUTPUT_KEY, OUTPUT_VALUE>> {
    public KeyValuePair<OUTPUT_KEY, OUTPUT_VALUE> reduce(KeyValuePair<KEY, List<VALUE>> input);
    // TODO Exclude these functions from user inherited map/reduce task.
}
