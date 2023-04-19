package com.ericgha.dao;

import com.ericgha.config.FunctionRedisTemplate;
import com.ericgha.config.RedisConfig;
import com.ericgha.dto.EventHash;
import com.ericgha.dto.EventTime;
import com.ericgha.dto.TimeIsValid;
import com.ericgha.test_fixtures.EnableRedisTestContainer;
import junit.framework.AssertionFailedError;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.lang.Nullable;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;

@EnableRedisTestContainer
@SpringBootTest(classes = {RedisConfig.class})
public class StrictlyOnceMapIntTest {

    private static final int EVENT_DURATION = 10_000;
    private static final String KEY_PREFIX = "STRICTLY_ONCE";
    private static final String CLOCK_ELEMENT = "CLOCK";
    @Autowired
    @Qualifier("stringLongRedisTemplate")
    FunctionRedisTemplate<String, Long> stringLongRedisTemplate;
    @Autowired
    RedisConnectionFactory connectionFactory;
    StrictlyOnceMap strictlyOnceMap;

    // expectedTImeAndIsValid of an empty list is equivilent to both values being null (this is mirroring how redis handles table null returns)
    static void assertConsistent(TimeIsValid expectedTV, @Nullable Long expectedRetired, EventHash found,
                                 String message) throws AssertionFailedError {
        Assertions.assertEquals( new EventHash( expectedTV.time(), expectedTV.isValid(), expectedRetired ), found,
                                 message );
    }

    @BeforeEach
    void before() {
        this.strictlyOnceMap =
                new StrictlyOnceMap( EVENT_DURATION, KEY_PREFIX, CLOCK_ELEMENT, stringLongRedisTemplate );
    }

    @AfterEach
    public void afterEach() {
        RedisConnection connection = connectionFactory.getConnection();
        connection.commands().flushAll();
    }

    @Test
    @DisplayName("putEvent with no recent events, expected return")
    void putEventNewEvent() {
        EventTime eventTime = new EventTime( "Test 1", Instant.now().toEpochMilli() );
        List<TimeIsValid> found = strictlyOnceMap.putEvent( eventTime );
        List<TimeIsValid> expected =
                List.of( new TimeIsValid( null, null ), new TimeIsValid( eventTime.time(), true ) );
        Assertions.assertEquals( expected, found );
        assertConsistent( found.get( 1 ), null, strictlyOnceMap.getEventHash( eventTime.event() ),
                          "expected hash state" );
        Assertions.assertEquals( 1, strictlyOnceMap.getClock() );
    }

    @Test
    @DisplayName(
            "Put duplicate events, first earlier, second later, no conflict, both validated recentEvents updated to later time")
    void putEventsFirstEarlierSecondLaterNoConflict() {
        EventTime firstEarlier = new EventTime( "Test 1", Instant.now().toEpochMilli() );
        strictlyOnceMap.putEvent( firstEarlier );
        EventTime secondLater = new EventTime( "Test 1", firstEarlier.time() + EVENT_DURATION );
        List<TimeIsValid> found = strictlyOnceMap.putEvent( secondLater );
        List<TimeIsValid> expected =
                List.of( new TimeIsValid( firstEarlier.time(), true ), new TimeIsValid( secondLater.time(), true ) );
        Assertions.assertEquals( expected, found, "expected return value" );
        assertConsistent( expected.get( 1 ), firstEarlier.time(), strictlyOnceMap.getEventHash( secondLater.event() ),
                          "expected hash state" );
        Assertions.assertEquals( 2, strictlyOnceMap.getClock(), "expected number of mutations" );
    }

    @Test
    @DisplayName("Put 2 duplicate events with same time, is_valid set from 1 to 0")
    void putEqualTimeEventsHaveConflictTest() {
        EventTime eventTime = new EventTime( "Test 1", Instant.now().toEpochMilli() );
        strictlyOnceMap.putEvent( eventTime );
        List<TimeIsValid> found = strictlyOnceMap.putEvent( eventTime );
        List<TimeIsValid> expected =
                List.of( new TimeIsValid( eventTime.time(), true ), new TimeIsValid( eventTime.time(), false ) );
        Assertions.assertEquals( expected, found, "expected return value" );
        assertConsistent( expected.get( 1 ), null, strictlyOnceMap.getEventHash( eventTime.event() ),
                          "expected hash state" );
        Assertions.assertEquals( 2, strictlyOnceMap.getClock(), "expected number of mutations" );
    }

    @Test
    @DisplayName(
            "Put 2 conflicting events, first earlier time second later time, time updated and is_valid 1 -> 0")
    void putLaterEventHasConflictTest() {
        EventTime firstEarlier = new EventTime( "Test 1", Instant.now().toEpochMilli() );
        EventTime secondLater = new EventTime( "Test 1", Instant.now().toEpochMilli() + 1 );
        strictlyOnceMap.putEvent( firstEarlier );
        List<TimeIsValid> found = strictlyOnceMap.putEvent( secondLater );
        List<TimeIsValid> expected =
                List.of( new TimeIsValid( firstEarlier.time(), true ), new TimeIsValid( secondLater.time(), false ) );
        Assertions.assertEquals( expected, found, "expected return value" );
        assertConsistent( expected.get( 1 ), null, strictlyOnceMap.getEventHash( firstEarlier.event() ),
                          "expected hash state" );
        Assertions.assertEquals( 2, strictlyOnceMap.getClock(), "2 mutations" );
    }

    @Test
    @DisplayName(
            "Put 2 conflicting events, first later time second earlier time, time not updated and is_valid 1 -> 0")
    void putEventConflictWhenPuttingEarlierEvent() {
        EventTime secondEarlier = new EventTime( "Test 1", Instant.now().toEpochMilli() );
        EventTime firstLater = new EventTime( "Test 1", Instant.now().toEpochMilli() + 1 );
        strictlyOnceMap.putEvent( firstLater ); // notice later put first
        List<TimeIsValid> found = strictlyOnceMap.putEvent( secondEarlier );
        List<TimeIsValid> expected =
                List.of( new TimeIsValid( firstLater.time(), true ), new TimeIsValid( firstLater.time(), false ) );
        Assertions.assertEquals( expected, found, "expected return value" );
        assertConsistent( expected.get( 1 ), null, strictlyOnceMap.getEventHash( firstLater.event() ),
                          "expected hash state" );
        Assertions.assertEquals( 2, strictlyOnceMap.getClock(), "expected number of mutations" );
    }

    @Test
    @DisplayName(
            "putEvent no conflict, second event earlier timestamp than first, no changes to hash")
    void putEventTwoEventsSecondEarlierThanFirstNoConflict() {
        EventTime firstLater = new EventTime( "Test 1", Instant.now().toEpochMilli() );
        strictlyOnceMap.putEvent( firstLater );
        EventTime secondEarlier = new EventTime( firstLater.event(), firstLater.time() - EVENT_DURATION );
        List<TimeIsValid> found = strictlyOnceMap.putEvent( secondEarlier );
        List<TimeIsValid> expected =
                List.of( new TimeIsValid( firstLater.time(), true ), new TimeIsValid( firstLater.time(), true ) );

        Assertions.assertEquals( expected, found, "expected return value" );
        assertConsistent( expected.get( 1 ), null, strictlyOnceMap.getEventHash( firstLater.event() ),
                          "expected hash state" );
        Assertions.assertEquals( 1, strictlyOnceMap.getClock(), "expected number of mutations" );
    }

    @Test
    @DisplayName(
            "putEvent, first earlier, second later, has conflict, update time and is_valid 1 -> 0")
    void putEventFirstEarlierSecondLaterHasConflict() {
        EventTime firstEarlier = new EventTime( "Test 1", Instant.now().toEpochMilli() );
        strictlyOnceMap.putEvent( firstEarlier );
        EventTime secondLater = new EventTime( "Test 1", firstEarlier.time() + 1 );
        List<TimeIsValid> found = strictlyOnceMap.putEvent( secondLater );
        List<TimeIsValid> expected =
                List.of( new TimeIsValid( firstEarlier.time(), true ), new TimeIsValid( secondLater.time(), false ) );

        Assertions.assertEquals( expected, found, "expected return value" );
        assertConsistent( expected.get( 1 ), null, strictlyOnceMap.getEventHash( firstEarlier.event() ),
                          "expected hash state" );
        Assertions.assertEquals( 2, strictlyOnceMap.getClock(), "expected number of mutations" );
    }

    @Test
    @DisplayName(
            "putEvent, first later, second earlier, has conflict, don't update time, is_valid 1 -> 0")
    void putEventFirstLaterSecondEarlierHasConflict() {
        EventTime secondEarlier = new EventTime( "Test 1", Instant.now().toEpochMilli() );
        EventTime firstLater = new EventTime( secondEarlier.event(), secondEarlier.time() + 1 );
        strictlyOnceMap.putEvent( firstLater );
        List<TimeIsValid> found = strictlyOnceMap.putEvent( secondEarlier );
        List<TimeIsValid> expected =
                List.of( new TimeIsValid( firstLater.time(), true ), new TimeIsValid( firstLater.time(), false ) );

        Assertions.assertEquals( expected, found, "expected return value" );
        assertConsistent( expected.get( 1 ), null, strictlyOnceMap.getEventHash( firstLater.event() ),
                          "expected hash state" );
        Assertions.assertEquals( 2, strictlyOnceMap.getClock(), "expected number of mutations" );
    }

    @Test
    @DisplayName("putEvent sets expiry time for hash")
    void putEventSetsExpiry() {
        EventTime eventTime = new EventTime( "Test 1", Instant.now().toEpochMilli() );
        strictlyOnceMap.putEvent( eventTime );
        Long expires_milli = stringLongRedisTemplate.getExpire( strictlyOnceMap.encodeEvent( eventTime.event() ),
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
        strictlyOnceMap.putEvent( firstEarliest );
        strictlyOnceMap.putEvent( secondMiddle );
        strictlyOnceMap.putEvent( thirdMiddle );
        EventHash foundState = strictlyOnceMap.getEventHash( firstEarliest.event() );
        EventHash expectedState = new EventHash( thirdMiddle.time(), false, firstEarliest.time() );
        Assertions.assertEquals( expectedState, foundState );
    }

}

