package com.ericgha.service.event_consumer;

import com.ericgha.dto.EventTime;
import com.ericgha.dto.Status;
import com.ericgha.dto.Versioned;
import com.ericgha.dto.message.PublishedEventMessage;
import com.ericgha.service.data.StrictlyOnceMapService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.simp.SimpMessagingTemplate;

/**
 * An event consumer which checks the validity of an event using an {@link StrictlyOnceMapService} if the event
 * is {@code Valid} a {@link PublishedEventMessage} is sent.  No message is sent for {@code Invalid} events.
 */
public class StrictlyOncePublisher implements EventConsumer {

    private final Logger log;
    private final StrictlyOnceMapService mapService;
    private final SimpMessagingTemplate messageTemplate;
    private final String messagePrefix;

    /**
     *
     * @param mapService
     * @param messageTemplate
     * @param messagePrefix where the messages should be sent.
     */
    public StrictlyOncePublisher(StrictlyOnceMapService mapService, SimpMessagingTemplate messageTemplate,
                                 String messagePrefix) {
        this.log =
                LoggerFactory.getLogger( String.format( "%s:%s", this.getClass().getName(), mapService.keyPrefix() ) );
        this.mapService = mapService;
        this.messageTemplate = messageTemplate;
        this.messagePrefix = messagePrefix;
    }

    @Override
    public void accept(Versioned<EventTime> versionedEventTime) {
        EventTime eventTime = versionedEventTime.data();
        Status foundStatus = mapService.isValid( eventTime );
        if ( foundStatus != Status.Valid) {
            log.debug( "Ignored a(n) {} event: {}.", foundStatus.name(), versionedEventTime );
        } else {
            long version = versionedEventTime.clock();
            PublishedEventMessage message = new PublishedEventMessage( version, eventTime );
            messageTemplate.convertAndSend( messagePrefix, message );
            log.debug( "Published event: " + versionedEventTime );
        }
    }
}
