package com.ericgha.config;

import com.ericgha.dao.EventQueue;
import com.ericgha.dao.StrictlyOnceMap;
import com.ericgha.domain.KeyMaker;
import com.ericgha.dto.EventStatus;
import com.ericgha.service.EventQueueSnapshotService;
import com.ericgha.service.EventService;
import com.ericgha.service.RateLimiter;
import com.ericgha.service.data.EventExpiryService;
import com.ericgha.service.data.EventQueueService;
import com.ericgha.service.data.StrictlyOnceMapService;
import com.ericgha.service.event_consumer.EventConsumer;
import com.ericgha.service.event_consumer.EventInvalidator;
import com.ericgha.service.event_consumer.StrictlyOncePublisher;
import com.ericgha.service.snapshot_consumer.SnapshotConsumer;
import com.ericgha.service.snapshot_consumer.SnapshotSTOMPMessenger;
import com.ericgha.service.snapshot_mapper.SnapshotMapper;
import com.ericgha.service.snapshot_mapper.ToSnapshotStatusCheckingValidity;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.simp.SimpMessagingTemplate;

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
    @Qualifier("strictlyOnceKeyMaker")
    KeyMaker strictlyOncekeyMaker() {
        return new KeyMaker( keyPrefix );
    }

    @Bean
    @Qualifier("strictlyOnceEventQueueService")
    EventQueueService strictlyOnceEventQueueService(EventQueue eventQueue,
                                                    @Qualifier("strictlyOnceKeyMaker") KeyMaker keyMaker) {

        return new EventQueueService( eventQueue, keyMaker );
    }

    @Bean
    @Qualifier("eventInvalidator")
    EventConsumer eventInvalidator(SimpMessagingTemplate messageTemplate) {
        return new EventInvalidator( stompPrefix, messageTemplate );
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
    @Qualifier("strictlyOnceEventPublisher")
    EventConsumer strictlyOnceEventPublisher(SimpMessagingTemplate messageTemplate,
                                             StrictlyOnceMapService eventMapService) {
        return new StrictlyOncePublisher( eventMapService, messageTemplate, stompPrefix );
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
    RateLimiter strictlyOnceEventService(StrictlyOnceMapService mapService,
                                         @Qualifier("strictlyOnceEventQueueService")
                                         EventQueueService eventQueueService,
                                         SimpMessagingTemplate simpMessagingTemplate) {
        return new EventService( stompPrefix, maxEvents, simpMessagingTemplate, eventQueueService,
                                 mapService );
    }

    @Bean
    @Qualifier("strictlyOnceSnapshotConsumer")
    SnapshotConsumer strictlyOnceSnapshotConsumer(StrictlyOnceMapService mapService,
                                                  SimpMessagingTemplate messagingTemplate) {
        SnapshotMapper<EventStatus> snapshotMapper = new ToSnapshotStatusCheckingValidity( mapService::isValid );
        return new SnapshotSTOMPMessenger( messagingTemplate, stompPrefix, snapshotMapper );
    }

    @Bean
    @Qualifier("strictlyOnceSnapshotService")
    EventQueueSnapshotService strictlyOnceSnapshotService(
            @Qualifier("strictlyOnceEventQueueService") EventQueueService strictlyOnceQueueService,
            @Qualifier("strictlyOnceSnapshotConsumer") SnapshotConsumer snapshotConsumer) {
        EventQueueSnapshotService snapshotService = new EventQueueSnapshotService( strictlyOnceQueueService );
        snapshotService.run( eventDuration, snapshotConsumer );
        return snapshotService;
    }
}
