package com.ericgha.service.event_consumer;

import com.ericgha.dto.EventTime;
import com.ericgha.dto.Versioned;
import com.ericgha.dto.message.PublishedEventMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.messaging.simp.SimpMessagingTemplate;

import java.time.Instant;

@ExtendWith(MockitoExtension.class)
class AlwaysPublishesEventConsumerTest {

    String messagePrefix = "/App/Test";
    @Mock
    SimpMessagingTemplate messageTemplate;
    AlwaysPublishesEventConsumer eventConsumer;

    @BeforeEach
    void before() {
        this.eventConsumer = new AlwaysPublishesEventConsumer( messageTemplate, messagePrefix );
    }

    @Test
    void accept() {
        EventTime eventTime = new EventTime( "testEvent", Instant.now().toEpochMilli() );
        Versioned<EventTime> versionedEvent = new Versioned<>( 1L, eventTime );
        eventConsumer.accept( versionedEvent );
        PublishedEventMessage expectedMessage = new PublishedEventMessage( versionedEvent.clock(), eventTime );
        Mockito.verify( messageTemplate )
                .convertAndSend( Mockito.eq( messagePrefix ), Mockito.eq( expectedMessage ) );
    }
}