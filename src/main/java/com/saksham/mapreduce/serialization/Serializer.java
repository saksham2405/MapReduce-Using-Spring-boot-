package com.saksham.mapreduce.serialization;

import com.fasterxml.jackson.core.JsonProcessingException;

import java.io.Serializable;

public interface Serializer<INPUT, OUTPUT> extends Serializable {

    public OUTPUT serialize(INPUT input) throws JsonProcessingException;
    public INPUT deserialize(OUTPUT output) throws JsonProcessingException;
}
