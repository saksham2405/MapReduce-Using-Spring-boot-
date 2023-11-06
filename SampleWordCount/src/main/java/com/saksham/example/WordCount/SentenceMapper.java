package com.saksham.example.WordCount;

import com.saksham.mapreduce.task.MapTask;
import com.saksham.mapreduce.types.KeyValuePair;

public class SentenceMapper extends MapTask<String> {
    @Override
    public KeyValuePair<?, ?> map(String s) {
        return new KeyValuePair<String, Integer>().setKey(s).setValue(1);
    }
}
