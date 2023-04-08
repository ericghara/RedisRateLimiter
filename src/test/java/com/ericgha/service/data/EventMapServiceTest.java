package com.ericgha.service.data;

import com.ericgha.config.RetryConfig;
import com.ericgha.dao.EventMap;
import exception.DirtyStateException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

@SpringBootTest(classes = {RetryConfig.class, EventMapService.class})
public class EventMapServiceTest {

    @MockBean
    EventMap eventMapMock;

    @Autowired
    EventMapService eventMapService;

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        // 5 invocations over approximately 5ms.  Should be approximately linear
        registry.add( "app.redis.retry.initial-interval", () -> 1 );
        registry.add( "app.redis.retry.multiplier", () -> 1.001 );
        registry.add( "app.redis.retry.num-attempts", () -> 5 );
    }

    @Test
    @DisplayName("Called num-attempts times and throws when all invocations fail")
    public void tryAddEventAllInvocationsFail() {
        when( eventMapMock.putEvent( Mockito.anyString(), Mockito.anyLong() ) ).thenThrow(
                new DirtyStateException( "Dummy Exception" ) );
        Assertions.assertThrows( DirtyStateException.class, () -> eventMapService.tryAddEvent( "testEvent", 123 ) );
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
        Assertions.assertTrue( eventMapService.tryAddEvent( "testEvent", 123 ) );
        Mockito.verify( eventMapMock, times( 3 ) ).putEvent( Mockito.anyString(), Mockito.anyLong() );
    }

    @Test
    @DisplayName("tryDeleteEvent calls EventMap#deleteEvent")
    public void tryDeleteEventCallsDeleteEvent() {
        Mockito.doReturn( true )
                .when( eventMapMock )
                .deleteEvent( Mockito.anyString(), Mockito.anyLong() );
        eventMapService.tryDeleteEvent( "testEvent", 123 );
        Mockito.verify( eventMapMock ).deleteEvent( Mockito.anyString(), Mockito.anyLong() );
    }
}
