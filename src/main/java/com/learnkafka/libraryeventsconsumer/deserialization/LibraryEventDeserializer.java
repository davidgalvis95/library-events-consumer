package com.learnkafka.libraryeventsconsumer.deserialization;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.libraryeventsconsumer.entity.LibraryEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

@Slf4j
public class LibraryEventDeserializer implements Deserializer<LibraryEvent> {

    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Deserializer.super.configure(configs, isKey);
    }

    @Override
    public LibraryEvent deserialize(String topic, byte[] data) {
        try {
            return objectMapper.readValue(new String(data, StandardCharsets.UTF_8), LibraryEvent.class);
        } catch (JsonProcessingException e) {
            log.error("Unable to deserialize message {}", data, e);
            return null;
        }
    }

//    @Override
//    public LibraryEvent deserialize(String topic, Headers headers, byte[] data) {
//        return Deserializer.super.deserialize(topic, headers, data);
//    }

    @Override
    public void close() {
        Deserializer.super.close();
    }
}
