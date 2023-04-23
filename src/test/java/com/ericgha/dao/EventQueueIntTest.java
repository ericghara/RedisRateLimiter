package com.ericgha.dao;

import com.ericgha.config.FunctionRedisTemplate;
import com.ericgha.config.RedisConfig;
import com.ericgha.domain.KeyMaker;
import com.ericgha.dto.EventTime;
import com.ericgha.dto.Versioned;
import com.ericgha.test_fixtures.EnableRedisTestContainer;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
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
public class EventQueueIntTest {

    KeyMaker keyMaker = new KeyMaker( "EventQueueIntTest" );
    @Autowired
    RedisConnectionFactory connectionFactory;
    @Autowired
    ObjectMapper objectMapper;
    @Autowired
    @Qualifier("stringTemplate")
    FunctionRedisTemplate<String, String> stringTemplate;

    EventQueue eventQueue;

    @BeforeEach
    void before() {
        eventQueue = new EventQueue( stringTemplate, objectMapper, keyMaker );
    }

    @AfterEach
    public void afterEach() {
        try (RedisConnection connection = connectionFactory.getConnection()) {
            connection.commands().flushAll();
        }
    }

    @Test
    public void offerAddsToEndOfQueue() {
        EventTime firstEvent = new EventTime( "first", 0 );
        EventTime secondEvent = new EventTime( "second", 1 );
        eventQueue.offer( firstEvent );
        eventQueue.offer( secondEvent );
        Assertions.assertEquals( firstEvent, eventQueue.poll() );
        Assertions.assertEquals( secondEvent, eventQueue.poll() );
    }

    @Test
    public void offerReturnsClock() {
        EventTime event0 = new EventTime( "zero", 0 );
        EventTime event1 = new EventTime( "one", 1 );
        long earlierClock = eventQueue.offer( event0 );
        long laterClock = eventQueue.offer( event1 );
        Assertions.assertTrue( earlierClock < laterClock, "Event added first has lower clock" );
    }

    @Test
    public void sizeReturnsExpected() {
        Assertions.assertEquals( 0L, eventQueue.size() );
        EventTime event = new EventTime( "first", 0 );
        eventQueue.offer( event );
        Assertions.assertEquals( 1L, eventQueue.size() );
    }

    @Test
    public void tryPollReturnsNullWhenQueueEmpty() {
        Assertions.assertNull( eventQueue.tryPoll( 1 ) );
    }

    @Test
    public void tryPollReturnsNullWhenHeadYoungerThanThreshold() {
        int threshold = 1;
        EventTime event = new EventTime( "Test Event", 2 ); // notice later time than threshold
        eventQueue.offer( event );
        Assertions.assertNull( eventQueue.tryPoll( threshold ) );
    }

    @Test
    public void tryPollReturnsEventWhenHeadSameAgeAsThreshold() {
        int threshold = 1;
        EventTime event = new EventTime( "Test Event", threshold ); // notice same time as threshold
        eventQueue.offer( event );
        Versioned<EventTime> versionedEvent = eventQueue.tryPoll( threshold );
        Assertions.assertEquals( event, versionedEvent.data(), "Event is expected" );
        Assertions.assertTrue( versionedEvent.clock() > 0, "clock > 0" );
    }

    @Test
    public void tryPollReturnsEventWhenHeadOlderThanThreshold() {
        int threshold = 1;
        EventTime event = new EventTime( "Test Event", 0 ); // notice older than threshold
        eventQueue.offer( event );
        Versioned<EventTime> versionedEvent = eventQueue.tryPoll( threshold );
        Assertions.assertEquals( event, versionedEvent.data(), "Event is expected" );
        Assertions.assertTrue( versionedEvent.clock() > 0, "clock > 0" );
    }

    @Test
    public void tryPollDoesNotIncrementClockWhenNoElementPolled() {
        EventTime event = new EventTime( "Test Event", 1 );
        eventQueue.offer( event );
        long expectedClock = eventQueue.getClock();
        eventQueue.tryPoll( 0 );
        long foundClock = eventQueue.getClock();
        Assertions.assertEquals( expectedClock, foundClock, "Clock should not change on a null poll." );
    }

    @Test
    public void getRangeEmptyQueue() {
        Assertions.assertEquals( List.of(), eventQueue.getRange( 0, -1 ).data() );
    }

    @Test
    public void getAllNonEmptyQueue() {
        EventTime event0 = new EventTime( "Test Event0", 0 );
        EventTime event1 = new EventTime( "Test Event1", 1 );
        List<EventTime> expectedEvents = List.of( event0, event1 );
        expectedEvents.forEach( eventQueue::offer );
        Versioned<List<EventTime>> versionedRange = eventQueue.getRange( 0, -1 );
        Assertions.assertEquals( expectedEvents, versionedRange.data() );
    }

    @Test
    public void operationsAreLinearized() {
        // where clock starts is not generally enforced, so just referencing a start point.
        long expectedVersion = eventQueue.getRange( 0, -1 ).clock();
        Assertions.assertEquals( ++expectedVersion, eventQueue.offer( new EventTime( "test", 0 ) ) );
        Assertions.assertEquals( ++expectedVersion, eventQueue.tryPoll( Instant.now().toEpochMilli() ).clock() );
        Assertions.assertEquals( ++expectedVersion, eventQueue.getRange( 0, -1 ).clock() );
    }
}
