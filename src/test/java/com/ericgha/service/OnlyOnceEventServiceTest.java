package com.ericgha.service;

import com.ericgha.dto.EventTime;
import com.ericgha.dto.Versioned;
import com.ericgha.dto.message.PublishedEventMessage;
import com.ericgha.exception.DirtyStateException;
import com.ericgha.service.data.EventMapService;
import com.ericgha.service.data.EventQueueService;
import com.ericgha.service.event_consumer.EventConsumer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.messaging.simp.SimpMessagingTemplate;

import java.time.Instant;

@ExtendWith(MockitoExtension.class)
public class OnlyOnceEventServiceTest {

    @Mock
    SimpMessagingTemplate msgTemplate;

    @Mock
    EventQueueService eventQueueService;

    @Mock
    EventMapService eventMapService;

    OnlyOnceEventService onlyOnceEventService;

    @BeforeEach
    void before() {
        String msgPrefix = "/topic/only-once";
        long maxEvents = 10;
        this.onlyOnceEventService =
                new OnlyOnceEventService( msgPrefix, maxEvents, msgTemplate, eventQueueService, eventMapService );
    }

    @Test
    @DisplayName("putEvent returns 507 when maxEvents exceeded")
    void putEventReturns507WhenTooManyEvents() {
        EventTime eventTime = new EventTime( "testEvent", Instant.now().toEpochMilli() );
        Mockito.doReturn( onlyOnceEventService.maxEvents() ).when( eventQueueService ).size();
        HttpStatus foundStatus = onlyOnceEventService.putEvent( eventTime );
        Assertions.assertEquals( HttpStatus.valueOf( 507 ), foundStatus );
    }

    @Test
    @DisplayName("putEvent returns 503 when eventMapService#tryAddEvent throws dirty state exception")
    void putEventReturns502WhenRetriesExhausted() {
        EventTime eventTime = new EventTime( "testEvent", Instant.now().toEpochMilli() );
        Mockito.doReturn( 0L ).when( eventQueueService ).size();
        Mockito.doThrow( new DirtyStateException() ).when( eventMapService )
                .tryAddEvent( Mockito.any( EventTime.class ) );
        HttpStatus foundStatus = onlyOnceEventService.putEvent( eventTime );
        Assertions.assertEquals( HttpStatus.valueOf( 503 ), foundStatus );
    }

    @Test
    @DisplayName("putEvent returns 201 when eventMapService#tryAddEvent returns true")
    void putEventReturns201WhenEventSuccessfullyPut() {
        EventTime eventTime = new EventTime( "testEvent", Instant.now().toEpochMilli() );
        Mockito.doReturn( 0L ).when( eventQueueService ).size();
        Mockito.doReturn( true ).when( eventMapService ).tryAddEvent( Mockito.any( EventTime.class ) );
        HttpStatus foundStatus = onlyOnceEventService.putEvent( eventTime );
        Assertions.assertEquals( HttpStatus.valueOf( 201 ), foundStatus );
    }

    @Test
    @DisplayName("putEvent offers event to eventQueue when eventMapService#tryAddEvent returns true")
    void putEventOffersToEventQueueWhenNoConflict() {
        EventTime eventTime = new EventTime( "testEvent", Instant.now().toEpochMilli() );
        Mockito.doReturn( 0L ).when( eventQueueService ).size();
        Mockito.doReturn( true ).when( eventMapService ).tryAddEvent( Mockito.any( EventTime.class ) );
        onlyOnceEventService.putEvent( eventTime );
        Mockito.verify( eventQueueService ).offer( eventTime );
    }

    @Test
    @DisplayName("putEvent returns 409 when eventMapService#tryAddEvent returns false")
    void putEventReturns409WhenEventNotPut() {
        EventTime eventTime = new EventTime( "testEvent", Instant.now().toEpochMilli() );
        Mockito.doReturn( 0L ).when( eventQueueService ).size();
        Mockito.doReturn( false ).when( eventMapService ).tryAddEvent( Mockito.any( EventTime.class ) );
        HttpStatus foundStatus = onlyOnceEventService.putEvent( eventTime );
        Assertions.assertEquals( HttpStatus.valueOf( 409 ), foundStatus );
    }

    @Test
    @DisplayName("eventConsumer publishes event")
    void eventConsumerPublishesEven() {
        EventTime eventTime = new EventTime( "testEvent", Instant.now().toEpochMilli() );
        Versioned<EventTime> versionedEvent = new Versioned<>( 1L, eventTime );
        EventConsumer eventConsumer = onlyOnceEventService.getEventConsumer();
        eventConsumer.accept( versionedEvent );
        PublishedEventMessage expectedMessage = new PublishedEventMessage( versionedEvent.clock(), eventTime );
        Mockito.verify( msgTemplate )
                .convertAndSend( Mockito.eq( onlyOnceEventService.messagePrefix() ), Mockito.eq( expectedMessage ) );
    }

    @Test
    @DisplayName("eventConsumer deletes event")
    void eventConsumerDeletesEvent() {
        EventTime eventTime = new EventTime( "testEvent", Instant.now().toEpochMilli() );
        Versioned<EventTime> versionedEvent = new Versioned<>( 1L, eventTime );
        EventConsumer eventConsumer = onlyOnceEventService.getEventConsumer();
        eventConsumer.accept( versionedEvent );
        Mockito.verify( eventMapService ).tryDeleteEvent( Mockito.eq( eventTime ) );
    }

}
