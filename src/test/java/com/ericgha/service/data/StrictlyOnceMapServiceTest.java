package com.ericgha.service.data;

import com.ericgha.dao.StrictlyOnceMap;
import com.ericgha.domain.KeyMaker;
import com.ericgha.dto.EventHash;
import com.ericgha.dto.EventTime;
import com.ericgha.dto.TimeIsValid;
import com.ericgha.dto.TimeIsValidDiff;
import com.ericgha.service.event_consumer.InMemoryEventStore;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.stream.Stream;

import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.mockStatic;

@ExtendWith(MockitoExtension.class)
class StrictlyOnceMapServiceTest {

    final static long EVENT_DURATION = 10_000L;
    final String KEY_PREFIX = "STRICTLY_ONCE_TEST";

    @Mock
    StrictlyOnceMap eventMap;
    StrictlyOnceMapService eventMapService;
    InMemoryEventStore invalidatedEventStore = new InMemoryEventStore();
    KeyMaker keyMaker = new KeyMaker( KEY_PREFIX );


    @BeforeEach
    void before() {
        this.eventMapService = new StrictlyOnceMapService( eventMap, EVENT_DURATION, keyMaker );
        this.eventMapService.setInvalidator( invalidatedEventStore );
    }

    @Test
    void putEventCallsEventMapWithExpectedEventKey() {
        EventTime event = new EventTime( "test 1", 0L );
        String expectedEventKey = keyMaker.generateEventKey( "test 1" );
        Mockito.doReturn( new TimeIsValidDiff( new TimeIsValid( 1L, true ), new TimeIsValid( 1L, false ), 2L ) )
                .when( eventMap )
                .putEvent( Mockito.anyString(), Mockito.anyLong(), Mockito.anyString(), Mockito.anyLong() );

        eventMapService.putEvent( event );
        Mockito.verify( eventMap, Mockito.description( "Called with proper eventKey and other args." ) )
                .putEvent( expectedEventKey, 0L, eventMapService.clockKey(), EVENT_DURATION );
    }

    static Stream<Arguments> returnValueTestSource() {

        return Stream.of(
                // format {diff, putTime (time we are trying to put), expected return, test label}
                arguments( new TimeIsValidDiff( new TimeIsValid( null, null ), new TimeIsValid( 0L, true ), 1L ), 0L,
                           true, "Empty Map, no conflict" ),
                arguments(
                        new TimeIsValidDiff( new TimeIsValid( 0L, true ), new TimeIsValid( EVENT_DURATION, true ), 2L ),
                        EVENT_DURATION, true, "First earlier is valid, Second Later, no conflict" ),
                arguments( new TimeIsValidDiff( new TimeIsValid( 0L, false ), new TimeIsValid( EVENT_DURATION, true ),
                                                2L ), EVENT_DURATION, true,
                           "First earlier not valid, Second Later, no conflict" ),
                arguments( new TimeIsValidDiff( new TimeIsValid( 0L, true ), new TimeIsValid( 1L, false ), 1L ), 1L,
                           false, "First is valid earlier, Second time later, has conflict" ),
                arguments( new TimeIsValidDiff( new TimeIsValid( 0L, false ), new TimeIsValid( 1L, false ), 1L ), 1L,
                           false, "First earlier is not valid, Second time later, has conflict" ),
                arguments( new TimeIsValidDiff( new TimeIsValid( 0L, true ), new TimeIsValid( 0L, false ), 1L ), 0L,
                           false, "First is valid, Second same time as first, conflict" ),
                arguments( new TimeIsValidDiff( new TimeIsValid( 0L, false ), new TimeIsValid( 0L, false ), null ), 0L,
                           false, "First is not valid, Second same time as first, conflict" ),
                arguments( new TimeIsValidDiff( new TimeIsValid( 1L, true ), new TimeIsValid( 1L, false ), 1L ), 0L,
                           false, "First later is valid, second time earlier, conflict" ),
                arguments( new TimeIsValidDiff( new TimeIsValid( 1L, false ), new TimeIsValid( 1L, false ), null ), 0L,
                           false, "First later is not valid, second time earlier, conflict" ),
                arguments( new TimeIsValidDiff( new TimeIsValid( EVENT_DURATION, true ),
                                                new TimeIsValid( EVENT_DURATION, true ), null ), 0L, false,
                           "First later is valid, Second time earlier than first, NO conflict" ),
                arguments( new TimeIsValidDiff( new TimeIsValid( EVENT_DURATION, false ),
                                                new TimeIsValid( EVENT_DURATION, false ), null ), 0L, false,
                           "First later is not valid, Second time earlier than first, NO conflict" )
        );
    }

    @ParameterizedTest(name = "[{index}] {3} - args: {0}, {1}")
    @MethodSource("returnValueTestSource")
    void putEventReturnValue(TimeIsValidDiff diff, Long putTime, Boolean expected, String _label) {
        Mockito.doReturn( diff ).when( eventMap )
                .putEvent( Mockito.anyString(), Mockito.anyLong(), Mockito.anyString(), Mockito.anyLong() );
        boolean found = eventMapService.putEvent( new EventTime( "_test", putTime ) );
        Assertions.assertEquals( expected, found );
    }

    @Test
    @DisplayName("putEvent submits invalidated event to invalidator (EventConsumer)")
    void putEventSubmitsInvalidatedEventToInvalidator() {
        Mockito.doReturn( new TimeIsValidDiff( new TimeIsValid( 1L, true ), new TimeIsValid( 1L, false ), 2L ) )
                .when( eventMap )
                .putEvent( Mockito.anyString(), Mockito.anyLong(), Mockito.anyString(), Mockito.anyLong() );
        eventMapService.putEvent( new EventTime( "Test", 0L ) );
        EventTime expectedInvalidated = new EventTime( "Test", 1L );
        Long foundVersion = invalidatedEventStore.eventsByVersionClock().get( expectedInvalidated );
        Assertions.assertNotNull( foundVersion, "Expected event invalidate" );
        Assertions.assertEquals( 2L, foundVersion, "Expected version" );
    }

    @Test
    @DisplayName("putEvent does not submit valid events to invalidator (EventConsumer)")
    void putEventDoesNotSubmitValidEventsToInvalidator() {
        Mockito.doReturn( new TimeIsValidDiff( new TimeIsValid( 1L, false ), new TimeIsValid( 1L, false ), null ) )
                .when( eventMap )
                .putEvent( Mockito.anyString(), Mockito.anyLong(), Mockito.anyString(), Mockito.anyLong() );
        eventMapService.putEvent( new EventTime( "Test", 0L ) );
        Assertions.assertTrue( invalidatedEventStore.eventsByVersionClock().isEmpty(), "No Event Submitted" );
    }

    static Stream<Arguments> isValidReturnTestSource() {
        return Stream.of(
                arguments( new EventHash( null, null, null ), 0, false, "empty hash" ),
                arguments( new EventHash( 0L, true, null ), 0, true, "query == time, is valid" ),
                arguments( new EventHash( 0L, false, null ), 0, false, "query == time, not valid" ),
                arguments( new EventHash( 0L, true, null ), 1, false, "query != time, is valid" ),
                arguments( new EventHash( 0L, false, null ), 1, false, "query != time, is not valid" ),
                arguments( new EventHash( 10L, true, 0L ), 0L, true, "query == retired, is valid" ),
                arguments( new EventHash( 10L, false, 0L ), 0L, true, "query == retired, is not valid" ),
                arguments( new EventHash( 10L, true, 0L ), 11L, false, "query != time or required, is valid" ),
                arguments( new EventHash( 10L, false, 0L ), 11L, false, "query != time or required, is not valid" )
        );

    }

    @ParameterizedTest(name = "[{index}] {3} - args: {0}, {1}")
    @MethodSource("isValidReturnTestSource")
    void isValidReturnsExpected(EventHash state, long timeQuery, boolean expected, String _label) {
        Mockito.doReturn( state ).when( eventMap ).getEventHash( Mockito.anyString() );
        // we mock time b/c EventMapService validates time of EventTime provided and throws if time is in future or far
        // in past
        Instant fakeInstant = Instant.ofEpochMilli( 2 * EVENT_DURATION );
        try (MockedStatic<Instant> instantMock = mockStatic( Instant.class, Mockito.CALLS_REAL_METHODS )) {
            instantMock.when( Instant::now ).thenReturn( fakeInstant );

            boolean found = eventMapService.isValid( new EventTime( "test 1", timeQuery ) );
            Assertions.assertEquals( expected, found, "expected return value" );
        }
    }

    @Test
    void isValidThrowsWhenTimeIsInTheFuture() {
        long futureTime = Instant.now().toEpochMilli()+1_000;
        EventTime futureEvent = new EventTime("Test 1", futureTime);
        Assertions.assertThrows(IllegalArgumentException.class, () -> eventMapService.isValid(futureEvent));
    }

    @Test
    void isValidThrowsWhenTimeGTDoubleEventDurationInPast() {
        long pastTime = Instant.now().toEpochMilli() - 2*EVENT_DURATION - 1;
        EventTime futureEvent = new EventTime("Test 1", pastTime);
        Assertions.assertThrows(IllegalArgumentException.class, () -> eventMapService.isValid(futureEvent));
    }
}