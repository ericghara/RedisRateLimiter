package com.ericgha.dao;

import com.ericgha.config.FunctionRedisTemplate;
import com.ericgha.config.RedisConfig;
import com.ericgha.dto.EventTime;
import com.ericgha.test_fixtures.EnableRedisTestContainer;
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

import java.time.Instant;
import java.util.List;

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
    @DisplayName("putEvent with no recent events, updates time isValid")
    void putEventNewEvent() {
        EventTime eventTime = new EventTime( "Test 1", Instant.now().toEpochMilli() );
        List<List<Long>> found = strictlyOnceMap.putEvent( eventTime );
        List<List<Long>> expected = List.of( List.of(), List.of( eventTime.time(), 1L ) );
        Assertions.assertEquals( expected, found );
        Assertions.assertEquals( found.get( 1 ), strictlyOnceMap.getEventInfo( eventTime.event() ) );
        Assertions.assertEquals( 1, strictlyOnceMap.getClock() );
    }

    @Test
    @DisplayName(
            "Put duplicate events, first earlier, second later, no conflict, both validated recentEvents updated to later time")
    void putEventsFirstEarlierSecondLaterNoConflict() {
        EventTime firstEarlier = new EventTime( "Test 1", Instant.now().toEpochMilli() );
        strictlyOnceMap.putEvent( firstEarlier );
        EventTime secondLater = new EventTime( "Test 1", firstEarlier.time() + EVENT_DURATION );
        List<List<Long>> found = strictlyOnceMap.putEvent( secondLater );
        List<List<Long>> expected = List.of( List.of( firstEarlier.time(), 1L ), List.of( secondLater.time(), 1L ) );
        Assertions.assertEquals( expected, found, "expected return value" );
        Assertions.assertEquals( expected.get( 1 ), strictlyOnceMap.getEventInfo( secondLater.event() ),
                                 "expected hash state" );
        Assertions.assertEquals( 2, strictlyOnceMap.getClock(), "expected number of mutations" );
    }

    @Test
    @DisplayName("Put 2 duplicate events with same time, is_valid set from 1 to 0")
    void putEqualTimeEventsHaveConflictTest() {
        EventTime eventTime = new EventTime( "Test 1", Instant.now().toEpochMilli() );
        strictlyOnceMap.putEvent( eventTime );
        List<List<Long>> found = strictlyOnceMap.putEvent( eventTime );
        List<List<Long>> expected = List.of( List.of( eventTime.time(), 1L ), List.of( eventTime.time(), 0L ) );
        Assertions.assertEquals( expected, found, "expected return value" );
        Assertions.assertEquals( expected.get( 1 ), strictlyOnceMap.getEventInfo( eventTime.event() ),
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
        List<List<Long>> found = strictlyOnceMap.putEvent( secondLater );
        List<List<Long>> expected = List.of( List.of( firstEarlier.time(), 1L ), List.of( secondLater.time(), 0L ) );
        Assertions.assertEquals( expected, found, "expected return value" );
        Assertions.assertEquals( expected.get( 1 ), strictlyOnceMap.getEventInfo( firstEarlier.event() ),
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
        List<List<Long>> found = strictlyOnceMap.putEvent( secondEarlier );
        List<List<Long>> expected = List.of( List.of( firstLater.time(), 1L ), List.of( firstLater.time(), 0L ) );
        Assertions.assertEquals( expected, found, "expected return value" );
        Assertions.assertEquals( expected.get( 1 ), strictlyOnceMap.getEventInfo( firstLater.event() ),
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
        List<List<Long>> found = strictlyOnceMap.putEvent( secondEarlier );
        List<List<Long>> expected = List.of( List.of( firstLater.time(), 1L ), List.of( firstLater.time(), 1L ) );

        Assertions.assertEquals( expected, found, "expected return value" );
        Assertions.assertEquals( expected.get( 1 ), strictlyOnceMap.getEventInfo( firstLater.event() ),
                                 "expected hash state" );
        Assertions.assertEquals( 1, strictlyOnceMap.getClock(), "expected number of mutations" );
    }

    @Test
    @DisplayName(
            "putEvent, first earlier, second later, has conflict, update time and is_valid 1 -> 0")
    void putEventFirstEarlierSecondLaterHasConflict() {
        EventTime firstEarlier = new EventTime( "Test 1", Instant.now().toEpochMilli() );
        strictlyOnceMap.putEvent( firstEarlier );
        EventTime secondLater = new EventTime( "Test 1", firstEarlier.time() + 1L );
        List<List<Long>> found = strictlyOnceMap.putEvent( secondLater );
        List<List<Long>> expected = List.of( List.of( firstEarlier.time(), 1L ), List.of( secondLater.time(), 0L ) );

        Assertions.assertEquals( expected, found, "expected return value" );
        Assertions.assertEquals( expected.get( 1 ), strictlyOnceMap.getEventInfo( firstEarlier.event() ),
                                 "expected hash state" );
        Assertions.assertEquals( 2, strictlyOnceMap.getClock(), "expected number of mutations" );
    }

    @Test
    @DisplayName(
            "putEvent, first later, second earlier, has conflict, don't update time, is_valid 1 -> 0")
    void putEventFirstLaterSecondEarlierHasConflict() {
        EventTime secondEarlier = new EventTime( "Test 1", Instant.now().toEpochMilli() );
        EventTime firstLater = new EventTime( secondEarlier.event(), secondEarlier.time() + 1L );
        strictlyOnceMap.putEvent( firstLater );
        List<List<Long>> found = strictlyOnceMap.putEvent( secondEarlier );
        List<List<Long>> expected = List.of( List.of( firstLater.time(), 1L ), List.of( firstLater.time(), 0L ) );

        Assertions.assertEquals( expected, found, "expected return value" );
        Assertions.assertEquals( expected.get( 1 ), strictlyOnceMap.getEventInfo( firstLater.event() ),
                                 "expected hash state" );
        Assertions.assertEquals( 2, strictlyOnceMap.getClock(), "expected number of mutations" );
    }
}

