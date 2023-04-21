package com.ericgha.config;

import com.ericgha.dao.EventQueue;
import com.ericgha.dao.StrictlyOnceMap;
import com.ericgha.domain.KeyMaker;
import com.ericgha.dto.EventStatus;
import com.ericgha.dto.EventTime;
import com.ericgha.service.EventQueueSnapshotService;
import com.ericgha.service.EventService;
import com.ericgha.service.data.EventExpiryService;
import com.ericgha.service.data.EventQueueService;
import com.ericgha.service.data.StrictlyOnceMapService;
import com.ericgha.service.event_consumer.EventConsumer;
import com.ericgha.service.event_consumer.EventInvalidator;
import com.ericgha.service.event_consumer.StrictlyOncePublisher;
import com.ericgha.service.snapshot_consumer.SnapshotConsumer;
import com.ericgha.service.snapshot_consumer.SnapshotSTOMPMessenger;
import com.ericgha.service.status_mapper.EventMapper;
import com.ericgha.service.status_mapper.ToEventStatusCheckingValidity;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.retry.support.RetryTemplate;

@Configuration
public class StrictlyOnceEventConfig {

    @Value("${app.web-socket.prefix.client}/${app.strictly-once-event.web-socket.element}")
    String stompPrefix;

    @Value("${app.strictly-once-event.event-duration-millis}")
    long eventDuration;

    @Value("${app.strictly-once-event.key-prefix}")
    String keyPrefix;

    @Value("${app.strictly-once-event.max-events}")
    Long maxEvents;

    @Bean
    StrictlyOnceMap strictlyOnceMap(
            @Qualifier("stringLongTemplate") FunctionRedisTemplate<String, Long> template) {
        return new StrictlyOnceMap( template );
    }

    @Bean
    @Qualifier("eventInvalidator")
    EventConsumer eventInvalidator(SimpMessagingTemplate messageTemplate) {
        return new EventInvalidator( stompPrefix, messageTemplate );
    }

    @Bean
    @Qualifier("strictlyOnceEventPublisher")
    EventConsumer strictlyOnceEventPublisher(SimpMessagingTemplate messageTemplate,
                                             StrictlyOnceMapService eventMapService) {
        return new StrictlyOncePublisher( eventMapService, messageTemplate, stompPrefix );
    }

    @Bean
    @Qualifier("StrictlyOnceSnapshotConsumer")
    SnapshotConsumer<EventStatus> strictlyOnceSnapshotConsumer(SimpMessagingTemplate messagingTemplate) {
        return new SnapshotSTOMPMessenger( messagingTemplate, stompPrefix );

    }

    @Bean
    @Qualifier("strictlyOnceKeyMaker")
    KeyMaker strictlyOncekeyMaker() {
        return new KeyMaker( keyPrefix );
    }

    @Bean
    StrictlyOnceMapService eventMapService(StrictlyOnceMap eventMap,
                                           @Qualifier("eventInvalidator") EventConsumer eventInvalidator,
                                           @Qualifier("strictlyOnceKeyMaker") KeyMaker keyMaker) {
        StrictlyOnceMapService mapService = new StrictlyOnceMapService( eventMap, eventDuration, keyMaker );
        mapService.setInvalidator( eventInvalidator );
        return mapService;
    }

    @Bean
    @Qualifier("strictlyOnceEventQueueService")
    EventQueueService strictlyOnceEventQueueService(FunctionRedisTemplate<String, EventTime> redisTemplate,
                                                    @Qualifier("eventQueueRetryTemplate") RetryTemplate retryTemplate,
                                                    @Qualifier("strictlyOnceKeyMaker") KeyMaker keyMaker) {
        // todo retry template should be global not per rate limiter
        EventQueue eventQueue = new EventQueue( redisTemplate, keyMaker );
        return new EventQueueService( eventQueue, retryTemplate );
    }

    @Bean
    @Qualifier("strictlyOnceEventExpiryService")
    EventExpiryService strictlyOnceEventExpiryService(
            @Qualifier("strictlyOnceEventQueueService") EventQueueService eventQueueService,
            @Qualifier("strictlyOnceEventPublisher") EventConsumer eventPublisher,
            @Value("${app.strictly-once-event.event-queue.num-workers}") int numWorkers) {
        EventExpiryService expiryService = new EventExpiryService( eventQueueService );
        expiryService.start( eventPublisher, eventDuration, numWorkers );
        return expiryService;
    }

    @Bean
    @Qualifier("strictlyOnceEventService")
    EventService strictlyOnceEventService(StrictlyOnceMapService mapService,
                                          @Qualifier("strictlyOnceEventQueueService")
                                                      EventQueueService eventQueueService,
                                          SimpMessagingTemplate simpMessagingTemplate) {
        return new EventService( stompPrefix, maxEvents, simpMessagingTemplate, eventQueueService,
                                 mapService );
    }

    @Bean
    @Qualifier("strictlyOnceSnapshotService")
    EventQueueSnapshotService strictlyOnceSnapshotService(
            @Qualifier("strictlyOnceEventQueueService") EventQueueService strictlyOnceQueueService,
            StrictlyOnceMapService strictlyOnceEventMapService,
            @Qualifier("strictlyOnceSnapshotConsumer") SnapshotConsumer<EventStatus> snapshotConsumer) {
        EventQueueSnapshotService snapshotService = new EventQueueSnapshotService( strictlyOnceQueueService );
        EventMapper<EventStatus> eventMapper =
                new ToEventStatusCheckingValidity( strictlyOnceEventMapService::isValid );
        snapshotService.run( eventDuration, eventMapper, snapshotConsumer );
        return snapshotService;
    }


}
