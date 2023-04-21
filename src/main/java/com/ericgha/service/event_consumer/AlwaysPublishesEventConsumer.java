package com.ericgha.service.event_consumer;

import com.ericgha.dto.EventTime;
import com.ericgha.dto.Versioned;
import com.ericgha.dto.message.PublishedEventMessage;
import com.ericgha.exception.DirtyStateException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.simp.SimpMessagingTemplate;

public class AlwaysPublishesEventConsumer implements EventConsumer {

    private final SimpMessagingTemplate messageTemplate;
    private final String messagePrefix;
    private final Logger log;

    public AlwaysPublishesEventConsumer(SimpMessagingTemplate messageTemplate, String messagePrefix) {
        this.log = LoggerFactory.getLogger( this.getClass().getName() );
        this.messageTemplate = messageTemplate;
        this.messagePrefix = messagePrefix;
    }

    @Override
    public void accept(Versioned<EventTime> versionedEventTime) {
        EventTime eventTime = versionedEventTime.data();
        PublishedEventMessage pubEventMessage = new PublishedEventMessage( versionedEventTime.clock(), eventTime );
        messageTemplate.convertAndSend( messagePrefix, pubEventMessage );
    }
}
