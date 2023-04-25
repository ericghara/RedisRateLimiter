package com.ericgha.service;

import com.ericgha.dto.EventTime;
import com.ericgha.dto.message.SubmittedEventMessage;
import com.ericgha.service.data.EventMapService;
import com.ericgha.service.data.EventQueueService;
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
class EventServiceTest {

    static final long MAX_EVENTS = 500;
    static final String MESSAGE_PREFIX = "/topic/test";

    @Mock
    SimpMessagingTemplate messageTemplate;

    @Mock
    EventQueueService queueService;

    @Mock
    EventMapService mapService;

    EventService eventService;

    @BeforeEach
    void before() {
        Mockito.doReturn( "test" ).when( mapService ).keyPrefix();
        eventService = new EventService( MESSAGE_PREFIX, MAX_EVENTS, messageTemplate, queueService, mapService );
    }

    @Test
    @DisplayName("acceptEvent returns 507 when EventQueueService#approxSize equals MAX_EVENTS")
    void acceptEventReturns507WhenQueueFull() {
        Mockito.doReturn( MAX_EVENTS ).when( queueService ).approxSize();
        EventTime event = new EventTime( "Test 1", Instant.now().toEpochMilli() );
        Assertions.assertEquals( HttpStatus.INSUFFICIENT_STORAGE, eventService.acceptEvent( event ) );
    }

    @Test
    @DisplayName("acceptEvent returns 409 when approxSize < MAX_EVENTS and mapService#putEvent returns false")
    void acceptEventReturns201WhenEventPutInMap() {
        Mockito.doReturn( 0L ).when( queueService ).approxSize();
        Mockito.doReturn( false ).when( mapService ).putEvent( Mockito.any( EventTime.class ) );
        EventTime event = new EventTime( "Test 1", Instant.now().toEpochMilli() );
        Assertions.assertEquals( HttpStatus.CONFLICT, eventService.acceptEvent( event ) );
    }

    @Test
    @DisplayName("acceptEvent returns 503 when an exception is thrown by putEvent")
    void acceptEventReturns503WhenAnyExceptionIsThrown() {
        Mockito.doReturn( 0L ).when( queueService ).approxSize();
        Mockito.doThrow( new IllegalStateException( "Boom!" ) ).when( mapService )
                .putEvent( Mockito.any( EventTime.class ) );
        EventTime event = new EventTime( "Test 1", Instant.now().toEpochMilli() );
        Assertions.assertEquals( HttpStatus.SERVICE_UNAVAILABLE, eventService.acceptEvent( event ) );
    }

    @Test
    @DisplayName("acceptEvent returns 201 when MapService#putEvent returns true")
    void acceptEventReturns201WhenPutEventReturnsTrue() {
        Mockito.doReturn( 0L ).when( queueService ).approxSize();
        Mockito.doReturn( true ).when( mapService ).putEvent( Mockito.any( EventTime.class ) );
        Mockito.doReturn( 1L ).when( queueService ).offer( Mockito.any( EventTime.class ) );
        EventTime event = new EventTime( "Test 1", Instant.now().toEpochMilli() );
        Assertions.assertEquals( HttpStatus.CREATED, eventService.acceptEvent( event ) );
    }

    @Test
    @DisplayName("acceptEvent sends a message if MapService#putEvent returns true")
    void acceptEventSendsAMessageIfItReturns201() {
        EventTime event = new EventTime( "Test 1", Instant.now().toEpochMilli() );
        final long clock = 1;
        Mockito.doReturn( 0L ).when( queueService ).approxSize();
        Mockito.doReturn( true ).when( mapService ).putEvent( Mockito.any( EventTime.class ) );
        Mockito.doReturn( clock ).when( queueService ).offer( Mockito.any( EventTime.class ) );
        SubmittedEventMessage expectedMessage = new SubmittedEventMessage( clock, event );
        eventService.acceptEvent( event );
        Mockito.verify( messageTemplate,
                        Mockito.times( 1 ).description( "A single message was sent with expected args" ) )
                .convertAndSend( MESSAGE_PREFIX, expectedMessage );
    }
}