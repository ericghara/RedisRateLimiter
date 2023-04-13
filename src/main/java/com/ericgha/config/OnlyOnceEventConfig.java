package com.ericgha.config;

import com.ericgha.dao.EventMap;
import com.ericgha.dao.EventQueue;
import com.ericgha.dto.EventStatus;
import com.ericgha.dto.EventTime;
import com.ericgha.exception.RetryableException;
import com.ericgha.service.EventQueueSnapshotService;
import com.ericgha.service.OnlyOnceEventService;
import com.ericgha.service.data.EventExpiryService;
import com.ericgha.service.data.EventMapService;
import com.ericgha.service.data.EventQueueService;
import com.ericgha.service.event_consumer.EventConsumer;
import com.ericgha.service.event_transformer.EventMapper;
import com.ericgha.service.event_transformer.ToEventStatusAlwaysValid;
import com.ericgha.service.snapshot_consumer.SnapshotConsumer;
import com.ericgha.service.snapshot_consumer.SnapshotSTOMPMessenger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
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

    @Value("${app.web-socket.prefix.client}/${app.once-only-event.web-socket.element}")
    String stompPrefix;
    @Value("${app.once-only-event.event-duration-millis}")
    long eventDuration;

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
    EventMapService onceOnlyEventMap(@Value("${app.once-only-event.event-map.key-prefix}") String keyPrefix,
                                     @Qualifier("eventMapRetryTemplate") RetryTemplate retryTemplate) {
        EventMap eventMap = new EventMap( stringRedisTemplate, eventDuration );
        return new EventMapService( eventMap, keyPrefix, retryTemplate );
    }

    @Bean
    @ConditionalOnProperty(name = "app.once-only-event.disable-bean.only-once-event-service", havingValue = "false",
            matchIfMissing = true)
    OnlyOnceEventService onceOnlyEventService(
            @Value("${app.once-only-event.max-events}") long maxEvents,
            SimpMessagingTemplate simpMessagingTemplate,
            @Qualifier("onceOnlyEventQueueService") EventQueueService eventQueueService,
            @Qualifier("onceOnlyEventMapService") EventMapService eventMapService) {
        return new OnlyOnceEventService( stompPrefix, maxEvents, simpMessagingTemplate, eventQueueService,
                                         eventMapService );
    }

    @Bean
    @Qualifier("onceOnlyEventExpiryService")
    @ConditionalOnProperty(name = "app.once-only-event.disable-bean.event-expiry-service", havingValue = "false",
            matchIfMissing = true)
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

    @Bean
    @Qualifier("onceOnlySnapshotService")
    @ConditionalOnProperty(name = "app.once-only-event.disable-bean.event-queue-snapshot-service",
            havingValue = "false", matchIfMissing = true)
    EventQueueSnapshotService snapshotService(
            SimpMessagingTemplate simpMessagingTemplate,
            @Qualifier("onceOnlyEventQueueService") EventQueueService eventQueueService) {
        EventQueueSnapshotService snapshotService = new EventQueueSnapshotService( eventQueueService );
        EventMapper<EventStatus> mapper = new ToEventStatusAlwaysValid();
        SnapshotConsumer<EventStatus> snapshotConsumer =
                new SnapshotSTOMPMessenger( simpMessagingTemplate, stompPrefix );
        snapshotService.run( eventDuration, mapper, snapshotConsumer );
        return snapshotService;
    }

}
