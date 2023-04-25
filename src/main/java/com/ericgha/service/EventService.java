package com.ericgha.service;

import com.ericgha.dto.EventTime;
import com.ericgha.dto.message.SubmittedEventMessage;
import com.ericgha.service.data.EventMapService;
import com.ericgha.service.data.EventQueueService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.messaging.simp.SimpMessagingTemplate;

/**
 * Handles intake of events.  Has the typical interface of a rate limiter where events are generally accepted or rejected.
 */
public class EventService implements RateLimiter {

    private final Logger log;
    private final String messagePrefix;
    private final long maxEvents;
    private final SimpMessagingTemplate msgTemplate;
    private final EventQueueService queueService;
    private final EventMapService mapService;


    /**
     *
     * <em>note</em> for efficiency queue size is approximated, ({@link EventQueueService#approxSize()})
     *
     * @param messagePrefix where {@link SubmittedEventMessage}s should be sent
     * @param maxEvents the approximate maximum size of the queue (irrespective of status)
     * @param msgTemplate the template used for messaging
     * @param queueService the queue this service should offer events to
     * @param mapService the map this service should manage
     */
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

    /**
     * @param eventTime event to try to add.
     * @return 507 InsufficientStorage: if queued events >= {@code maxEvents}, 503 Service Unavailable: any error occurs,
     * 201 Created: if event accepted, 409 Conflict: if the {@link EventMapService} rejects the event.
     * {@code eventDuration}
     */
    @Override
    public HttpStatus acceptEvent(EventTime eventTime) {
        if (queueService.approxSize() >= maxEvents) {
            return HttpStatus.INSUFFICIENT_STORAGE;
        }
        try {
            boolean success = mapService.putEvent( eventTime );
            if (success) {
                long clock = queueService.offer( eventTime );
                SubmittedEventMessage submittedEventMessage = new SubmittedEventMessage( clock, eventTime );
                msgTemplate.convertAndSend( messagePrefix, submittedEventMessage );
                return HttpStatus.CREATED;
            }
        } catch (Exception e) {
            log.info("Encountered an error while accepting: {}", eventTime);
            log.debug("Error while accepting {}: {}", eventTime, e);
            return HttpStatus.SERVICE_UNAVAILABLE;
        }
        return HttpStatus.CONFLICT;
    }
}
