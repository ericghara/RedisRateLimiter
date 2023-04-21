package com.ericgha.service.event_consumer;

import com.ericgha.dto.EventTime;
import com.ericgha.dto.Versioned;
import com.ericgha.dto.message.InvalidatedEventMessage;
import org.springframework.messaging.simp.SimpMessagingTemplate;

public class EventInvalidator implements EventConsumer {

    private final SimpMessagingTemplate messageTemplate;
    private final String messagePrefix;

    public EventInvalidator(String messagePrefix, SimpMessagingTemplate messageTemplate) {
        this.messageTemplate = messageTemplate;
        this.messagePrefix = messagePrefix;
    }

    @Override public void accept(Versioned<EventTime> versionedEventTime) {
        long version = versionedEventTime.clock();
        EventTime eventTime = versionedEventTime.data();
        InvalidatedEventMessage message = new InvalidatedEventMessage( version, eventTime );
        messageTemplate.convertAndSend( messagePrefix, message );
    }
}
