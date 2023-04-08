package com.ericgha.service;

import com.ericgha.dto.EventTime;
import com.ericgha.dto.message.AddedEventMessage;
import com.ericgha.dto.message.PublishedEventMessage;
import com.ericgha.service.data.EventMapService;
import com.ericgha.service.data.EventQueueService;
import com.ericgha.service.event_consumer.EventConsumer;
import exception.DirtyStateException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.messaging.simp.SimpMessagingTemplate;

import java.time.Instant;

public class OnlyOnceEventService {

    String prefix;
    long maxEvents;
    SimpMessagingTemplate msgTemplate;
    EventQueueService eventQueueService;
    EventMapService eventMapService;
    Logger log;

    public OnlyOnceEventService(String prefix, long maxEvents, SimpMessagingTemplate msgTemplate, EventQueueService eventQueueService, EventMapService eventMapService) {
        this.log = LoggerFactory.getLogger( this.getClass().getName() );
        this.prefix = prefix;
        this.maxEvents = maxEvents;
        this.msgTemplate = msgTemplate;
        this.eventQueueService = eventQueueService;
        this.eventMapService = eventMapService;
    }

    /**
     * @param eventTime
     * @return 507 InsufficientStorage: if queued events >= {@code maxEvents}, 503 Service Unavailable: if retries
     * exhausted, 201 Created: if event accepted, 409 Conflict: if an identical event occurred within
     * {@code eventDuration}
     */
    public HttpStatus putEvent(EventTime eventTime) {
        if (eventQueueService.size() >= maxEvents) {
            return HttpStatus.INSUFFICIENT_STORAGE;
        }
        boolean success;
        try {
            success = eventMapService.tryAddEvent( eventTime );
        } catch (DirtyStateException e) {
            log.debug( "Exhausted retries: {}", eventTime );
            return HttpStatus.SERVICE_UNAVAILABLE;
        }
        long timestamp = Instant.now().toEpochMilli();
        if (success) {
            AddedEventMessage addedEventMessage = new AddedEventMessage( timestamp, eventTime );
            msgTemplate.convertAndSend( prefix, addedEventMessage );
            return HttpStatus.CREATED;
        }
        return HttpStatus.CONFLICT;
    }

    public EventConsumer getEventConsumer() {
        return (EventTime eventTime) -> {
            try {
                eventMapService.tryDeleteEvent( eventTime );
            } catch (DirtyStateException e) {
                log.info( "Unable to delete event from EventMap: {}", eventTime );
            }
            long time = Instant.now().toEpochMilli();
            PublishedEventMessage pubEventMessage = new PublishedEventMessage( time, eventTime );
            msgTemplate.convertAndSend( prefix, pubEventMessage );
        };

    }


}
