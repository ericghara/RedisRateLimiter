package com.ericgha.config;

import com.ericgha.dao.EventQueue;
import com.ericgha.dao.OnlyOnceMap;
import com.ericgha.domain.KeyMaker;
import com.ericgha.dto.EventStatus;
import com.ericgha.service.EventQueueSnapshotService;
import com.ericgha.service.EventService;
import com.ericgha.service.RateLimiter;
import com.ericgha.service.data.EventExpiryService;
import com.ericgha.service.data.EventQueueService;
import com.ericgha.service.data.FunctionRedisTemplate;
import com.ericgha.service.data.OnlyOnceEventMapService;
import com.ericgha.service.event_consumer.AlwaysPublishesEventConsumer;
import com.ericgha.service.event_consumer.EventConsumer;
import com.ericgha.service.snapshot_consumer.SnapshotSTOMPMessenger;
import com.ericgha.service.snapshot_mapper.SnapshotMapper;
import com.ericgha.service.snapshot_mapper.ToSnapshotStatusAlwaysValid;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.simp.SimpMessagingTemplate;

@Configuration
public class OnlyOnceEventConfig {

    @Autowired
    @Qualifier("stringTemplate")
    FunctionRedisTemplate<String, String> stringTemplate;

    @Value("${app.web-socket.prefix.client}/${app.only-once-event.web-socket.element}")
    String stompPrefix;
    @Value("${app.only-once-event.event-duration-millis}")
    long eventDurationMilli;
    @Value("${app.only-once-event.key-prefix}")
    String keyPrefix;

    @Bean
    @Qualifier("onlyOnceKeyMaker")
    KeyMaker keyMaker() {
        return new KeyMaker( keyPrefix );
    }

    @Bean
    @Qualifier("onlyOnceEventQueueService")
    EventQueueService onlyOnceEventQueue(@Qualifier("onlyOnceKeyMaker") KeyMaker keyMaker,
                                         EventQueue eventQueue) {
        return new EventQueueService( eventQueue, keyMaker );
    }

    @Bean
    @Qualifier("onlyOnceEventMapService")
    OnlyOnceEventMapService onlyOnceEventMap(@Qualifier("onlyOnceKeyMaker") KeyMaker keyMaker,
                                             OnlyOnceMap eventMap) {
        return new OnlyOnceEventMapService( eventMap, eventDurationMilli, keyMaker );
    }

    @Bean
    @Qualifier("onlyOnceEventPublisher")
    @ConditionalOnProperty(name = "app.only-once-event.disable-bean.event-publisher", havingValue = "false",
            matchIfMissing = true)
    EventConsumer onlyOnceEventPublisher(SimpMessagingTemplate messageTemplate) {
        return new AlwaysPublishesEventConsumer( messageTemplate, stompPrefix );
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

    // this only requires the queue for snapshotting and publication of events at end time, an only once EventService
    // w/o these requirements could be implemented without the queue
    @Bean
    @Qualifier("onlyOnceEventService")
    @ConditionalOnProperty(name = "app.only-once-event.disable-bean.event-service", havingValue = "false",
            matchIfMissing = true)
    RateLimiter onlyOnceEventService(
            @Value("${app.only-once-event.max-events}") long maxEvents,
            SimpMessagingTemplate simpMessagingTemplate,
            @Qualifier("onlyOnceEventQueueService") EventQueueService eventQueueService,
            @Qualifier("onlyOnceEventMapService") OnlyOnceEventMapService eventMapService) {
        return new EventService( stompPrefix, maxEvents, simpMessagingTemplate, eventQueueService, eventMapService );
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
        snapshotService.run( eventDurationMilli, snapshotConsumer );
        return snapshotService;
    }

}
