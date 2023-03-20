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
    @Qualifier("RedisRetryTemplate")
    RetryTemplate retry(@Value("${app.redis.retry.initial-interval}") long initialInterval,
                        @Value("${app.redis.retry.multiplier}") double multiplier,
                        @Value("${app.redis.retry.num-attempts}") int numAttempts) {
        return RetryTemplate.builder()
                .exponentialBackoff( initialInterval, multiplier, 3_600_000, true )
                .maxAttempts( numAttempts )
                .retryOn( RetryableException.class )
                .build();
    }
}
