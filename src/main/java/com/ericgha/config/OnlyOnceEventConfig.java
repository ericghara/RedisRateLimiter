package com.ericgha.config;

import com.ericgha.dao.EventQueue;
import com.ericgha.dao.OnlyOnceMap;
import com.ericgha.domain.KeyMaker;
import com.ericgha.dto.EventStatus;
import com.ericgha.service.EventQueueSnapshotService;
import com.ericgha.service.EventService;
import com.ericgha.service.data.EventExpiryService;
import com.ericgha.service.data.EventQueueService;
import com.ericgha.service.data.OnlyOnceEventMapService;
import com.ericgha.service.event_consumer.AlwaysPublishesEventConsumer;
import com.ericgha.service.event_consumer.EventConsumer;
import com.ericgha.service.snapshot_consumer.SnapshotSTOMPMessenger;
import com.ericgha.service.snapshot_mapper.SnapshotMapper;
import com.ericgha.service.snapshot_mapper.ToSnapshotStatusAlwaysValid;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.retry.annotation.EnableRetry;

@Configuration
@EnableRetry
public class OnlyOnceEventConfig {

    @Autowired
    @Qualifier("stringTemplate")
    FunctionRedisTemplate<String, String> stringTemplate;

    @Value("${app.web-socket.prefix.client}/${app.only-once-event.web-socket.element}")
    String stompPrefix;
    @Value("${app.only-once-event.event-duration-millis}")
    long eventDuration;
    @Value("${app.only-once-event.key-prefix}")
    String keyPrefix;

    @Bean
    @Qualifier("onlyOnceKeyMaker")
    KeyMaker keyMaker() {
        return new KeyMaker( keyPrefix );
    }

    // todo move these to their own config class

    @Bean
    @Qualifier("onlyOnceEventQueueService")
    EventQueueService onlyOnceEventQueue(@Qualifier("onlyOnceKeyMaker") KeyMaker keyMaker,
                                         ObjectMapper objectMapper) {
        EventQueue eventQueue = new EventQueue( stringTemplate, objectMapper );
        return new EventQueueService( eventQueue, keyMaker );
    }

    @Bean
    @Qualifier("onlyOnceEventMapService")
    OnlyOnceEventMapService onlyOnceEventMap(@Qualifier("onlyOnceKeyMaker") KeyMaker keyMaker) {
        OnlyOnceMap eventMap = new OnlyOnceMap( stringTemplate );
        return new OnlyOnceEventMapService( eventMap, eventDuration, keyMaker );
    }

    @Bean
    @Qualifier("onlyOnceEventPublisher")
    @ConditionalOnProperty(name = "app.only-once-event.disable-bean.event-publisher", havingValue = "false",
            matchIfMissing = true)
    EventConsumer onlyOnceEventPublisher(SimpMessagingTemplate messageTemplate) {
        return new AlwaysPublishesEventConsumer( messageTemplate, stompPrefix );
    }

    // this only requires the queue for snapshotting and publication of events at end time, an only once EventService
    // w/o these requirements could be implemented without the queue
    @Bean
    @Qualifier("onlyOnceEventService")
    @ConditionalOnProperty(name = "app.only-once-event.disable-bean.event-service", havingValue = "false",
            matchIfMissing = true)
    EventService onlyOnceEventService(
            @Value("${app.only-once-event.max-events}") long maxEvents,
            SimpMessagingTemplate simpMessagingTemplate,
            @Qualifier("onlyOnceEventQueueService") EventQueueService eventQueueService,
            @Qualifier("onlyOnceEventMapService") OnlyOnceEventMapService eventMapService) {
        return new EventService( stompPrefix, maxEvents, simpMessagingTemplate, eventQueueService, eventMapService );
    }

    @Bean
    @Qualifier("onlyOnceEventExpiryService")
    @ConditionalOnProperty(name = "app.only-once-event.disable-bean.event-expiry-service", havingValue = "false",
            matchIfMissing = true)
    EventExpiryService eventExpiryService(
            @Value("${app.only-once-event.event-duration-millis}") int eventDuration,
            @Value("${app.only-once-event.event-queue.num-workers}") int numWorkers,
            @Qualifier("onlyOnceEventPublisher") EventConsumer eventPublisher,
            @Qualifier("onlyOnceEventQueueService") EventQueueService eventQueueService) {
        EventExpiryService expiryService = new EventExpiryService( eventQueueService );
        expiryService.start( eventPublisher, eventDuration, numWorkers );
        return expiryService;
    }

    @Bean
    @Qualifier("onlyOnceSnapshotService")
    @ConditionalOnProperty(name = "app.only-once-event.disable-bean.event-queue-snapshot-service",
            havingValue = "false", matchIfMissing = true)
    EventQueueSnapshotService snapshotService(
            SimpMessagingTemplate simpMessagingTemplate,
            @Qualifier("onlyOnceEventQueueService") EventQueueService eventQueueService) {
        EventQueueSnapshotService snapshotService = new EventQueueSnapshotService( eventQueueService );
        SnapshotMapper<EventStatus> mapper = new ToSnapshotStatusAlwaysValid();
        SnapshotSTOMPMessenger snapshotConsumer =
                new SnapshotSTOMPMessenger( simpMessagingTemplate, stompPrefix, mapper );
        snapshotConsumer.chunkSize( Integer.MAX_VALUE );
        snapshotService.run( eventDuration, snapshotConsumer );
        return snapshotService;
    }

}
