package com.ericgha.config;

import com.ericgha.dao.EventMap;
import com.ericgha.dao.EventQueue;
import com.ericgha.dto.EventTime;
import com.ericgha.service.OnlyOnceEventService;
import com.ericgha.service.data.EventExpiryService;
import com.ericgha.service.data.EventMapService;
import com.ericgha.service.data.EventQueueService;
import com.ericgha.service.event_consumer.EventConsumer;
import exception.RetryableException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.retry.support.RetryTemplate;

@Configuration
public class OnlyOnceEventConfig {

    @Autowired
    @Qualifier("eventTimeRedisTemplate")
    RedisTemplate<String, EventTime> eventTimeRedisTemplate;

    @Autowired
    StringRedisTemplate stringRedisTemplate;

    @Bean
    @Qualifier("eventMapRetryTemplate")
    RetryTemplate eventMapRetry(@Value("${app.once-only-event.event-map.retry.initial-interval}") long initialInterval,
                                @Value("${app.once-only-event.event-map.retry.multiplier}") double multiplier,
                                @Value("${app.once-only-event.event-map.retry.num-attempts}") int numAttempts) {
        return RetryTemplate.builder()
                .exponentialBackoff( initialInterval, multiplier, 3_600_000, true )
                .maxAttempts( numAttempts )
                .retryOn( RetryableException.class )
                .build();
    }

    @Bean
    @Qualifier("eventQueueRetryTemplate")
    RetryTemplate eventQueueRetry(@Value("${app.once-only-event.event-queue.retry.num-attempts}") int numAttempts,
                                  @Value("${app.once-only-event.event-queue.retry.interval}") long interval) {
        return RetryTemplate.builder()
                .fixedBackoff( interval )
                .maxAttempts( numAttempts )
                .retryOn( RetryableException.class )
                .build();
    }

    @Bean
    @Qualifier("onceOnlyEventQueueService")
    EventQueueService onceOnlyEventQueue(@Value("${app.once-only-event.event-queue.queue-id}") String queueId,
                                         @Qualifier("eventQueueRetryTemplate") RetryTemplate retryTemplate) {
        EventQueue eventQueue = new EventQueue( eventTimeRedisTemplate, queueId );
        return new EventQueueService( eventQueue, retryTemplate );
    }

    @Bean
    @Qualifier("onceOnlyEventMapService")
    EventMapService onceOnlyEventMap(@Value("${app.once-only-event.event-duration-millis}") long eventDuration,
                                     @Qualifier("eventMapRetryTemplate") RetryTemplate retryTemplate) {
        EventMap eventMap = new EventMap( stringRedisTemplate, eventDuration );
        return new EventMapService( eventMap, retryTemplate );
    }

    @Bean
    OnlyOnceEventService onceOnlyEventService(
            @Value("${app.web-socket.prefix.client}/${app.once-only-event.web-socket.element}") String prefix,
            @Value("${app.once-only-event.max-events}") long maxEvents,
            SimpMessagingTemplate simpMessagingTemplate,
            @Qualifier("onceOnlyEventQueueService") EventQueueService eventQueueService,
            @Qualifier("onceOnlyEventMapService") EventMapService eventMapService) {
        return new OnlyOnceEventService( prefix, maxEvents, simpMessagingTemplate, eventQueueService, eventMapService );
    }

    @Bean
    @Qualifier("OnlyOnceEventExpiryService")
    EventExpiryService eventExpiryService(
            @Value("${app.once-only-event.event-duration-millis}") int eventDuration,
            @Value("${app.once-only-event.event-queue.num-workers}") int numWorkers,
            @Qualifier("onceOnlyEventQueueService") EventQueueService eventQueueService,
            OnlyOnceEventService onlyOnceEventService) {
        EventConsumer eventConsumer = onlyOnceEventService.getEventConsumer();
        EventExpiryService expiryService = new EventExpiryService( eventQueueService );
        expiryService.start( eventConsumer, eventDuration, numWorkers );
        return expiryService;
    }

}