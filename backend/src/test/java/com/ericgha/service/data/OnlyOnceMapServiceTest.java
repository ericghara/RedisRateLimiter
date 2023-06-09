package com.ericgha.service.data;

import com.ericgha.config.OnlyOnceEventConfig;
import com.ericgha.dao.EventQueue;
import com.ericgha.dao.OnlyOnceMap;
import com.ericgha.domain.KeyMaker;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.time.Instant;

@SpringBootTest(classes = {OnlyOnceEventConfig.class})
public class OnlyOnceMapServiceTest {
    @MockBean
    EventQueue queue;  // just mocked to load context

    @MockBean
    @Qualifier("stringTemplate")
    FunctionRedisTemplate<String, String> stringTemplate;


    @MockBean
    OnlyOnceMap eventMapMock;


    @Autowired
    @Qualifier("onlyOnceKeyMaker")
    KeyMaker keyMaker;

    long EVENT_DURATION = 1_000L;

    OnlyOnceEventMapService eventMapService;

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        // 5 invocations over approximately 5ms.  Should be approximately linear
        // disable beans
        registry.add( "app.only-once-event.disable-bean.event-expiry-service", () -> true );
        registry.add( "app.only-once-event.disable-bean.event-queue-snapshot-service", () -> true );
        registry.add( "app.only-once-event.disable-bean.event-service", () -> true );
        registry.add( "app.only-once-event.disable-bean.event-publisher", () -> true );
    }

    @BeforeEach
    void before() {
        this.eventMapService = new OnlyOnceEventMapService( eventMapMock, EVENT_DURATION, keyMaker );
    }

    @Test
    @DisplayName("putEvent calls OnlyOnceMap#putEvent with the expected args")
    public void putEventUsesExpectedKey() {
        Mockito.doReturn( true ).when( eventMapMock )
                .putEvent( Mockito.anyString(), Mockito.anyLong(), Mockito.anyLong() );
        String event = "testEvent";
        long timeMilli = Instant.now().toEpochMilli();
        Assertions.assertTrue( eventMapService.putEvent( "testEvent", timeMilli ), "expected return" );
        String expectedKey = keyMaker.generateEventKey( event );
        Mockito.verify( eventMapMock ).putEvent( expectedKey, timeMilli, timeMilli + EVENT_DURATION );
    }

    @Test
    @DisplayName("putEvent returns false and does not involve DAO for an already ended event")
    public void putEventReturnsFalseOnExpiredEvent() {
        Mockito.doThrow( new AssertionError( "No interaction should occur with DAO, but DAO WAS called." ) )
                .when( eventMapMock ).putEvent( Mockito.anyString(), Mockito.anyLong(), Mockito.anyLong() );
        String event = "testEvent";
        long pastTime = Instant.now().toEpochMilli() - EVENT_DURATION;
        Assertions.assertFalse( eventMapService.putEvent( event, pastTime ) );
    }

    @Test
    @DisplayName("putEvent returns false and does not involve DAO for an event in the future")
    public void putEventReturnsFalseForEventInFuture() {
        Mockito.doThrow( new AssertionError( "No interaction should occur with DAO, but DAO WAS called." ) )
                .when( eventMapMock ).putEvent( Mockito.anyString(), Mockito.anyLong(), Mockito.anyLong() );
        String event = "testEvent";
        long pastTime = Instant.now().toEpochMilli() + 10;
        Assertions.assertFalse( eventMapService.putEvent( event, pastTime ) );
    }
}
