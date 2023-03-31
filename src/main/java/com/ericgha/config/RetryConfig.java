package com.ericgha.config;

import exception.RetryableException;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.retry.support.RetryTemplate;

@Configuration
public class RetryConfig {

    @Bean
    @Qualifier("eventMapRetryTemplate")
    RetryTemplate eventMapRetry(@Value("${app.event-map.retry.initial-interval}") long initialInterval,
                        @Value("${app.event-map.retry.multiplier}") double multiplier,
                        @Value("${app.event-map.retry.num-attempts}") int numAttempts) {
        return RetryTemplate.builder()
                .exponentialBackoff( initialInterval, multiplier, 3_600_000, true )
                .maxAttempts( numAttempts )
                .retryOn( RetryableException.class )
                .build();
    }

    @Bean
    @Qualifier("eventQueueRetryTemplate")
    RetryTemplate eventQueueRetry(@Value("${app.event-queue.retry.num-attempts}") int numAttempts,
                        @Value("${app.event-queue.retry.interval}") long interval) {
        return RetryTemplate.builder()
                .fixedBackoff( interval )
                .maxAttempts( numAttempts )
                .retryOn( RetryableException.class )
                .build();
    }
}
