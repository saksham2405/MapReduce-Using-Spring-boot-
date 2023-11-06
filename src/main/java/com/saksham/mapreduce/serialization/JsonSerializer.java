package com.saksham.mapreduce.serialization;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.lang.reflect.Type;

@Slf4j
@Component
public class JsonSerializer<INPUT> implements Serializer<INPUT, String> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public String serialize(INPUT input) throws JsonProcessingException {
        return objectMapper.writeValueAsString(input);
    }

    @Override
    public INPUT deserialize(String string) throws JsonProcessingException {
            return objectMapper.readValue(string, new TypeReference<INPUT>() {
            });
    }
}
