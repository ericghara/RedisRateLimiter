package com.ericgha.dto.message;

import com.ericgha.dto.EventTime;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(classes = {ObjectMapper.class})
class EventStatusMessageInterfaceTest {

    @Autowired
    ObjectMapper objectMapper;

    @Test
    void serializationTest() throws JsonProcessingException {
        SubmittedEventMessage submittedEventMessage = new SubmittedEventMessage( 0, new EventTime( "test", 1 ) );
        String jsonStr = objectMapper.writer().writeValueAsString( submittedEventMessage );
        String expectedStr = """
                {"timestamp":0,"eventTime":{"event":"test","time":1},"messageType":"SUBMITTED_EVENT"}""";
        Assertions.assertEquals(expectedStr, jsonStr);
    }

}