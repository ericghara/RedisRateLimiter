package com.ericgha.service;

import com.ericgha.dto.EventTime;
import com.ericgha.dto.Versioned;
import com.ericgha.dto.message.SubmittedEventMessage;
import com.ericgha.dto.message.PublishedEventMessage;
import com.ericgha.exception.DirtyStateException;
import com.ericgha.service.data.OnlyOnceEventMapService;
import com.ericgha.service.data.EventQueueService;
import com.ericgha.service.event_consumer.EventConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.messaging.simp.SimpMessagingTemplate;

public class OnlyOnceEventService {

    private final String messagePrefix;
    private final long maxEvents;
    private final SimpMessagingTemplate msgTemplate;
    private final EventQueueService eventQueueService;
    private final OnlyOnceEventMapService eventMapService;
    private final Logger log;

    public OnlyOnceEventService(String messagePrefix, long maxEvents, SimpMessagingTemplate msgTemplate,
                                EventQueueService eventQueueService, OnlyOnceEventMapService eventMapService) {
        this.log = LoggerFactory.getLogger( this.getClass().getName() );
        this.messagePrefix = messagePrefix;
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
            success = eventMapService.putEvent( eventTime );
        } catch (DirtyStateException e) {
            log.debug( "Exhausted retries: {}", eventTime );
            return HttpStatus.SERVICE_UNAVAILABLE;
        }
        if (success) {
            long clock = eventQueueService.offer( eventTime );
            SubmittedEventMessage submittedEventMessage = new SubmittedEventMessage( clock, eventTime );
            msgTemplate.convertAndSend( messagePrefix, submittedEventMessage );
            return HttpStatus.CREATED;
        }
        return HttpStatus.CONFLICT;
    }

    public EventConsumer getEventConsumer() {
        return (Versioned<EventTime> versionedEvent) -> {
            EventTime eventTime = versionedEvent.data();
            try {
                eventMapService.tryDeleteEvent( eventTime );
            } catch (DirtyStateException e) {
                log.info( "Unable to delete event from EventMapService: {}", versionedEvent );
            }
            PublishedEventMessage pubEventMessage = new PublishedEventMessage( versionedEvent.clock(), eventTime );
            msgTemplate.convertAndSend( messagePrefix, pubEventMessage );
        };

    }

    public long maxEvents() {
        return this.maxEvents;
    }

    public String messagePrefix() {
        return messagePrefix;
    }


}
