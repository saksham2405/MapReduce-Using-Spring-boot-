package com.saksham.example.WordCount;

import com.saksham.mapreduce.task.ReduceTask;
import com.saksham.mapreduce.types.KeyValuePair;

import java.util.List;

public class WordCountReducer extends ReduceTask<String, Integer, String, Integer> {
    @Override
    public KeyValuePair<String, Integer> reduce(KeyValuePair<String, List<Integer>> keyValuePair) {
        int sum = 0;
        for (Integer count : keyValuePair.getValue()) {
            sum += count;
        }
        return new KeyValuePair<String, Integer>().setKey(keyValuePair.getKey()).setValue(sum);
    }
}
