package com.saksham.mapreduce.serialization;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.saksham.mapreduce.types.KeyValuePair;
import com.saksham.mapreduce.types.WorkItem;

import java.util.HashMap;

public class KeyValuePairSerializer<KEY, VALUE> implements Serializer<KeyValuePair<KEY, VALUE>, String> {


    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public String serialize(KeyValuePair<KEY, VALUE> keyValuePair) throws JsonProcessingException {
        return objectMapper.writeValueAsString(keyValuePair);

    }

    @Override
    public KeyValuePair<KEY, VALUE> deserialize(String s) throws JsonProcessingException {

        return objectMapper.readValue(s, new TypeReference<KeyValuePair<KEY, VALUE>>() {});
    }
}
