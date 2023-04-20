package com.ericgha.dao;

import com.ericgha.config.FunctionRedisTemplate;
import com.ericgha.config.RedisConfig;
import com.ericgha.dto.EventHash;
import com.ericgha.dto.EventTime;
import com.ericgha.dto.TimeIsValid;
import com.ericgha.dto.TimeIsValidDiff;
import com.ericgha.test_fixtures.EnableRedisTestContainer;
import junit.framework.AssertionFailedError;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.lang.Nullable;

import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.jupiter.params.provider.Arguments.arguments;

@EnableRedisTestContainer
@SpringBootTest(classes = {RedisConfig.class})
public class StrictlyOnceMapIntTest {

    private static final long EVENT_DURATION = 10_000;
    private static final String CLOCK_KEY = "CLOCK";
    @Autowired
    @Qualifier("stringLongRedisTemplate")
    FunctionRedisTemplate<String, Long> stringLongRedisTemplate;
    @Autowired
    RedisConnectionFactory connectionFactory;

    StrictlyOnceMap strictlyOnceMap;

    @BeforeEach
    public void beforeEach() {
        this.strictlyOnceMap = new StrictlyOnceMap(stringLongRedisTemplate);
    }

    @AfterEach
    public void afterEach() {
        RedisConnection connection = connectionFactory.getConnection();
        connection.commands().flushAll();
    }

    static Stream<Arguments> returnValueTestSource() {

        return Stream.of(
                // format {expected return diff, second time, test label}
                arguments(new TimeIsValidDiff( new TimeIsValid( null, null ), new TimeIsValid( 0L, true ), 1L ), 0L, "Empty Map, no conflict"),
                arguments( new TimeIsValidDiff( new TimeIsValid( 0L, true ), new TimeIsValid( EVENT_DURATION, true ), 1L ), EVENT_DURATION, "First earlier is valid, Second Later, no conflict"),
                arguments( new TimeIsValidDiff( new TimeIsValid( 0L, false ), new TimeIsValid( EVENT_DURATION, true ), 1L ), EVENT_DURATION, "First earlier not valid, Second Later, no conflict"),
                arguments(new TimeIsValidDiff(new TimeIsValid( 0L, true ), new TimeIsValid( 1L, false ), 1L ), 1L, "First is valid earlier, Second time later, has conflict"),
                arguments(new TimeIsValidDiff(new TimeIsValid( 0L, false ), new TimeIsValid( 1L, false ), 1L ), 1L, "First earlier is not valid, Second time later, has conflict"),
                arguments(new TimeIsValidDiff( new TimeIsValid( 0L, true ), new TimeIsValid( 0L, false ), 1L ), 0L, "First is valid, Second same time as first, conflict"),
                arguments(new TimeIsValidDiff( new TimeIsValid( 0L, false ), new TimeIsValid( 0L, false ), null ), 0L, "First is not valid, Second same time as first, conflict"),
                arguments(new TimeIsValidDiff(new TimeIsValid( 1L, true ), new TimeIsValid( 1L, false ), 1L ), 0L, "First later is valid, second time earlier, conflict"),
                arguments(new TimeIsValidDiff(new TimeIsValid( 1L, false ), new TimeIsValid( 1L, false ), null ), 0L, "First later is not valid, second time earlier, conflict"),
                arguments( new TimeIsValidDiff( new TimeIsValid( EVENT_DURATION, true ), new TimeIsValid( EVENT_DURATION, true ), null ), 0L, "First later is valid, Second time earlier than first, NO conflict"),
                arguments( new TimeIsValidDiff( new TimeIsValid( EVENT_DURATION, false ), new TimeIsValid( EVENT_DURATION, false ), null ), 0L, "First later is not valid, Second time earlier than first, NO conflict")
        );
    }

    @ParameterizedTest(name = "[{index}]) {2}")
    @MethodSource("returnValueTestSource")
    void putEventReturnValue(TimeIsValidDiff expected, long secondTime, String _label) {
        String event = "Test 1";
        Long firstTime = expected.previous().time();
        if (Objects.nonNull(firstTime)) {
            strictlyOnceMap.setEvent( event, firstTime, expected.previous().isValid(), EVENT_DURATION );
        }
        TimeIsValidDiff found = strictlyOnceMap.putEvent( event, secondTime, CLOCK_KEY, EVENT_DURATION );
        Assertions.assertEquals(expected, found, "Expected return value.");
    }

    static Stream<Arguments> eventHashTestSource() {

        return Stream.of(
                // format {intial state, time to put, expected end state}
                arguments(new TimeIsValid(null, null), 0L, new EventHash(0L, true, null), "Empty Map, no conflict"),
                arguments(new TimeIsValid(0L, true), EVENT_DURATION, new EventHash(EVENT_DURATION, true, 0L), "First earlier is valid, Second Later, no conflict"),
                arguments(new TimeIsValid(0L, false), EVENT_DURATION, new EventHash(EVENT_DURATION, true, null), "First earlier not valid, Second Later, no conflict"),
                arguments(new TimeIsValid(0L, true), 1L, new EventHash(1L, false, null), "First is valid earlier, Second time later, has conflict"),
                arguments(new TimeIsValid(0L, false), 1L, new EventHash(1L, false, null), "First earlier is not valid, Second time later, has conflict"),
                arguments(new TimeIsValid(0L, true), 0L, new EventHash(0L, false, null), "First is valid, Second same time as first, conflict"),
                arguments(new TimeIsValid(0L, false), 0L, new EventHash(0L, false, null), "First is not valid, Second same time as first, conflict"),
                arguments(new TimeIsValid(1L, true), 0L, new EventHash(1L, false, null), "First later is valid, second time earlier, conflict"),
                arguments(new TimeIsValid(1L, false), 0L, new EventHash(1L, false, null), "First later is not valid, second time earlier, conflict"),
                arguments(new TimeIsValid(EVENT_DURATION, true), 0L, new EventHash(EVENT_DURATION, true, null), "First later is valid, Second time earlier than first, NO conflict"),
                arguments(new TimeIsValid(EVENT_DURATION, false), 0L, new EventHash(EVENT_DURATION, false, null), "First later is not valid, Second time earlier than first, NO conflict")
                );
    }

    @ParameterizedTest(name = "[{index}]) {3}")
    @MethodSource("eventHashTestSource")
    void putEventExpectedHashState(TimeIsValid initialState, long secondTime, EventHash expectedFinal, String _label) {
        String eventKey = "Test 1";
        Long firstTime = initialState.time();
        if (Objects.nonNull(firstTime)) {
            strictlyOnceMap.setEvent( eventKey, firstTime, initialState.isValid(), EVENT_DURATION );
        }
        strictlyOnceMap.putEvent(eventKey, secondTime, CLOCK_KEY, EVENT_DURATION);
        EventHash foundHash = strictlyOnceMap.getEventHash( eventKey );
        Assertions.assertEquals(expectedFinal, foundHash, "Redish event hash state");
    }

    static Stream<Arguments> clockStateTestSource() {

        return Stream.of(
                // format {expected return diff, second time, test label}
                arguments(new TimeIsValid( null, null ), new EventTime( "Test 1", 0L ), 1L, "Empty Map, no conflict"),
                arguments( new TimeIsValid( 0L, true ), new EventTime( "Test 1", EVENT_DURATION ), 1L, "First earlier is valid, Second Later, no conflict"),
                arguments( new TimeIsValid( 0L, false ), new EventTime( "Test 1", EVENT_DURATION ), 1L, "First earlier not valid, Second Later, no conflict"),
                arguments(new TimeIsValid( 0L, true ), new EventTime( "Test 1", 1L ), 1L, "First is valid earlier, Second time later, has conflict"),
                arguments(new TimeIsValid( 0L, false ), new EventTime( "Test 1", 1L ), 1L, "First earlier is not valid, Second time later, has conflict"),
                arguments(new TimeIsValid( 0L, true ), new EventTime( "Test 1", 0L), 1L, "First is valid, Second same time as first, conflict"),
                arguments(new TimeIsValid( 0L, false ), new EventTime( "Test 1", 0L), 0L, "First is not valid, Second same time as first, conflict"),
                arguments(new TimeIsValid( 1L, true ), new EventTime( "Test 1",0L), 1L, "First later is valid, second time earlier, conflict"),
                arguments(new TimeIsValid( 1L, false ), new EventTime("Test 1",0L), 0L, "First later is not valid, second time earlier, conflict"),
                arguments( new TimeIsValid( EVENT_DURATION, true ), new EventTime( "Test 1", 0L ), 0L, "First later is valid, Second time earlier than first, NO conflict"),
                arguments( new TimeIsValid( EVENT_DURATION, false ), new EventTime( "Test 1", 0L ), 0L, "First later is not valid, Second time earlier than first, NO conflict")
        );
    }

    @ParameterizedTest(name = "[{index}]) {3}")
    @MethodSource("clockStateTestSource")
    void putEventCorrectClockState(TimeIsValid timeIsValid, EventTime nextEvent, long expectedClock, String _label) {
        String eventKey = nextEvent.event();
        if (Objects.nonNull(timeIsValid.time())) {
            strictlyOnceMap.setEvent( eventKey, timeIsValid.time(), timeIsValid.isValid(), EVENT_DURATION );
        }
        stringLongRedisTemplate.opsForValue().set(CLOCK_KEY, 0L);
        strictlyOnceMap.putEvent( eventKey, nextEvent.time(), CLOCK_KEY, EVENT_DURATION );
        Assertions.assertEquals(expectedClock, strictlyOnceMap.getClock(CLOCK_KEY),"Expected clock state.");

    }

    @Test
    @DisplayName("putEvent sets expiry time for hash")
    void putEventSetsExpiry() {
        EventTime eventTime = new EventTime( "Test 1", Instant.now().toEpochMilli() );
        strictlyOnceMap.putEvent( eventTime.event(), eventTime.time(), CLOCK_KEY, EVENT_DURATION );
        Long expires_milli = stringLongRedisTemplate.getExpire( eventTime.event(),
                                                                TimeUnit.MILLISECONDS );
        Assertions.assertTrue( expires_milli <= 2 * EVENT_DURATION, "Not greater than initially set expire time" );
        Assertions.assertTrue( expires_milli >= 2 * EVENT_DURATION - 100,
                               "is greater than initial expire time - 100 ms" ); // generously allow 100 ms for querying
    }

    @Test
    @DisplayName("putEvent eventHash.is_valid unchanged by conflicts")
    void putEventIsValidUnchangedByConflicts() {
        EventTime firstEarliest = new EventTime( "Test 1", Instant.now().toEpochMilli() );
        EventTime secondMiddle = new EventTime( "Test 1", firstEarliest.time() + EVENT_DURATION );
        EventTime thirdMiddle = new EventTime( "Test 1", secondMiddle.time() ); // conflict
        strictlyOnceMap.putEvent( firstEarliest.event(), firstEarliest.time(), CLOCK_KEY, EVENT_DURATION );
        strictlyOnceMap.putEvent( secondMiddle.event(), secondMiddle.time(), CLOCK_KEY, EVENT_DURATION );
        strictlyOnceMap.putEvent( thirdMiddle.event(), thirdMiddle.time(), CLOCK_KEY, EVENT_DURATION );
        EventHash foundState = strictlyOnceMap.getEventHash( firstEarliest.event() );
        EventHash expectedState = new EventHash( thirdMiddle.time(), false, firstEarliest.time() );
        Assertions.assertEquals( expectedState, foundState );
    }

}

