package com.ericgha.service;

import com.ericgha.dto.EventTime;
import com.ericgha.dto.message.SubmittedEventMessage;
import com.ericgha.service.data.EventMapService;
import com.ericgha.service.data.EventQueueService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.messaging.simp.SimpMessagingTemplate;

public class EventService implements RateLimiter {

    private final Logger log;
    private final String messagePrefix;
    private final long maxEvents;
    private final SimpMessagingTemplate msgTemplate;
    private final EventQueueService queueService;
    private final EventMapService mapService;


    public EventService(String messagePrefix, long maxEvents, SimpMessagingTemplate msgTemplate,
                        EventQueueService queueService, EventMapService mapService) {
        this.log =
                LoggerFactory.getLogger( String.format( "%s:%s", this.getClass().getName(), mapService.keyPrefix() ) );
        this.messagePrefix = messagePrefix;
        this.maxEvents = maxEvents;
        this.msgTemplate = msgTemplate;
        this.queueService = queueService;
        this.mapService = mapService;
    }

    @Override
    public HttpStatus acceptEvent(EventTime eventTime) {
        if (queueService.size() >= maxEvents) {  // todo remove this check and integrate into putEvent
            return HttpStatus.INSUFFICIENT_STORAGE;
        }
        boolean success;
        try {
            success = mapService.putEvent( eventTime );
        } catch (Exception e) {
            log.info("Encountered an error while accepting: {}", eventTime);
            log.debug("Error while accepting {}: {}", eventTime, e);
            return HttpStatus.SERVICE_UNAVAILABLE;
        }
        if (success) {
            long clock = queueService.offer( eventTime );
            SubmittedEventMessage submittedEventMessage = new SubmittedEventMessage( clock, eventTime );
            msgTemplate.convertAndSend( messagePrefix, submittedEventMessage );
            return HttpStatus.CREATED;
        }
        return HttpStatus.CONFLICT;
    }
}
