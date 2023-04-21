package com.ericgha.service.data;

import com.ericgha.config.FunctionRedisTemplate;
import com.ericgha.config.OnlyOnceEventConfig;
import com.ericgha.config.RedisConfig;
import com.ericgha.dao.OnlyOnceMap;
import com.ericgha.domain.KeyMaker;
import com.ericgha.dto.EventTime;
import com.ericgha.exception.DirtyStateException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

@SpringBootTest(classes = {RedisConfig.class, OnlyOnceEventConfig.class})
public class OnlyOnceMapServiceTest {

    @MockBean
    OnlyOnceMap eventMapMock;

    @MockBean
    @Qualifier("stringTemplate")
    FunctionRedisTemplate<String, String> stringTemplate;

    @MockBean
    @Qualifier("eventTimeTemplate")
    FunctionRedisTemplate<String, EventTime> eventTimeTemplate;

    @Autowired
    @Qualifier("onlyOnceKeyMaker")
    KeyMaker keyMaker;

    OnlyOnceEventMapService eventMapService;

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        // 5 invocations over approximately 5ms.  Should be approximately linear
        registry.add( "app.redis.retry.initial-interval", () -> 1 );
        registry.add( "app.redis.retry.multiplier", () -> 1.001 );
        registry.add( "app.redis.retry.num-attempts", () -> 5 );
        // disable beans
        registry.add( "app.redis.disable-bean.redis-connection-factory", () -> true );
        registry.add( "app.redis.disable-bean.string-redis-template", () -> true );
        registry.add( "app.redis.disable-bean.event-time-redis-template", () -> true );
        registry.add( "app.redis.disable-bean.string-long-redis-template", () -> true );
        registry.add( "app.only-once-event.disable-bean.event-expiry-service", () -> true );
        registry.add( "app.only-once-event.disable-bean.event-queue-snapshot-service", () -> true );
        registry.add( "app.only-once-event.disable-bean.only-once-event-service", () -> true );
    }

    @BeforeEach
    void before(@Qualifier("eventMapRetry") RetryTemplate retryTemplate) {
        this.eventMapService = new OnlyOnceEventMapService( eventMapMock, keyMaker, retryTemplate );
    }

    @Test
    @DisplayName("Called num-attempts times and throws when all invocations fail")
    public void tryAddEventAllInvocationsFail() {
        when( eventMapMock.putEvent( Mockito.anyString(), Mockito.anyLong() ) ).thenThrow(
                new DirtyStateException( "Dummy Exception" ) );
        Assertions.assertThrows( DirtyStateException.class, () -> eventMapService.putEvent( "testEvent", 123 ) );
        Mockito.verify( eventMapMock, times( 5 ) ).putEvent( Mockito.anyString(), Mockito.anyLong() );
    }

    @Test
    @DisplayName("Calls DAO 3 times when first two throw and third succeeds and returns expected")
    public void tryAddEventReturnsExpectedWhenThirdSucceeds() {
        Mockito.doThrow( new DirtyStateException() )
                .doThrow( new DirtyStateException() )
                .doReturn( true )
                .when( eventMapMock )
                .putEvent( Mockito.anyString(), Mockito.anyLong() );
        Assertions.assertTrue( eventMapService.putEvent( "testEvent", 123 ) );
        Mockito.verify( eventMapMock, times( 3 ) ).putEvent( Mockito.anyString(), Mockito.anyLong() );
    }

    @Test
    @DisplayName("putEvent calls OnlyOnceMap#putEvent with the expected key")
    public void tryAddEventUsesExpectedKey() {
        Mockito.doReturn( true ).when( eventMapMock ).putEvent( Mockito.anyString(), Mockito.anyLong() );
        String event = "testEvent";
        long timeMilli = 123;
        eventMapService.putEvent( "testEvent", timeMilli );
        String expectedKey = keyMaker.generateEventKey( event );
        Mockito.verify( eventMapMock ).putEvent( expectedKey, timeMilli );
    }

    @Test
    @DisplayName("tryDeleteEvent calls OnlyOnceMap#deleteEvent")
    public void tryDeleteEventCallsDeleteEvent() {
        Mockito.doReturn( true )
                .when( eventMapMock )
                .deleteEvent( Mockito.anyString(), Mockito.anyLong() );
        eventMapService.tryDeleteEvent( "testEvent", 123 );
        Mockito.verify( eventMapMock ).deleteEvent( Mockito.anyString(), Mockito.anyLong() );
    }

    @Test
    @DisplayName("tryDeleteEvent calls OnlyOnceMap#deleteEvent with the expected key")
    public void tryDeleteEventUsesExpectedKey() {
        Mockito.doReturn( true )
                .when( eventMapMock )
                .deleteEvent( Mockito.anyString(), Mockito.anyLong() );
        String event = "testEvent";
        long timeMilli = 123;
        eventMapService.tryDeleteEvent( "testEvent", timeMilli );
        String expectedKey = keyMaker.generateEventKey( event );
        Mockito.verify( eventMapMock ).deleteEvent( expectedKey, timeMilli );
    }
}
