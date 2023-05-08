package com.ericgha.config;

import com.ericgha.dao.EventQueue;
import com.ericgha.dao.OnlyOnceMap;
import com.ericgha.dao.StrictlyOnceMap;
import com.ericgha.service.data.FunctionRedisTemplate;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.retry.annotation.EnableRetry;

@Configuration
@EnableRetry
public class DaoConfig {

    @Bean
    ObjectMapper objectMapper() {
        return new ObjectMapper();
    }

    @Bean
    EventQueue eventQueue(@Qualifier("stringTemplate") FunctionRedisTemplate<String, String> stringTemplate,
                          ObjectMapper objectMapper) {
        return new EventQueue( stringTemplate, objectMapper );
    }

    @Bean
    OnlyOnceMap onlyOnceMap(@Qualifier("stringTemplate") FunctionRedisTemplate<String, String> stringTemplate) {
        return new OnlyOnceMap( stringTemplate );
    }

    @Bean
    StrictlyOnceMap strictlyOnceMap(
            @Qualifier("stringLongTemplate") FunctionRedisTemplate<String, Long> stringLongTemplate) {
        return new StrictlyOnceMap( stringLongTemplate );
    }

}
