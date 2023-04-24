package com.ericgha.dao;

import com.ericgha.config.FunctionRedisTemplate;
import com.ericgha.config.RedisConfig;
import com.ericgha.dto.EventTime;
import com.ericgha.dto.PollResponse;
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

    @Autowired
    RedisConnectionFactory connectionFactory;
    @Autowired
    ObjectMapper objectMapper;
    @Autowired
    @Qualifier("stringTemplate")
    FunctionRedisTemplate<String, String> stringTemplate;

    EventQueue eventQueue;

    String clockKey = "test:CLOCK";
    String queueKey = "test:QUEUE";

    @BeforeEach
    void before() {
        eventQueue = new EventQueue( stringTemplate, objectMapper );
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
        eventQueue.offer( firstEvent, queueKey, clockKey );
        eventQueue.offer( secondEvent, queueKey, clockKey );
        Assertions.assertEquals( firstEvent, eventQueue.poll( queueKey ) );
        Assertions.assertEquals( secondEvent, eventQueue.poll( queueKey ) );
    }

    @Test
    public void offerReturnsIncreasingClocks() {
        EventTime event0 = new EventTime( "zero", 0 );
        EventTime event1 = new EventTime( "one", 1 );
        Versioned<Long> earlierResult = eventQueue.offer( event0, queueKey, clockKey );
        Versioned<Long> laterResult = eventQueue.offer( event1, queueKey, clockKey );
        Assertions.assertTrue( earlierResult.clock() < laterResult.clock(), "Event added first has lower clock" );
    }

    @Test
    public void offerReturnsExpectedSize() {
        EventTime event0 = new EventTime( "zero", 0 );
        Versioned<Long> earlierResult = eventQueue.offer( event0, queueKey, clockKey );
        Assertions.assertEquals( 1L, earlierResult.data() );
        EventTime event1 = new EventTime( "one", 1 );
        Versioned<Long> laterResult = eventQueue.offer( event1, queueKey, clockKey );
        Assertions.assertEquals( 2L, laterResult.data() );
    }

    @Test
    public void sizeReturnsExpected() {
        Assertions.assertEquals( 0L, eventQueue.size( queueKey ) );
        EventTime event = new EventTime( "first", 0 );
        eventQueue.offer( event, queueKey, clockKey );
        Assertions.assertEquals( 1L, eventQueue.size( queueKey ) );
    }

    @Test
    public void tryPollReturnsOnlyListSizeWhenQueueEmpty() {
        int threshold = 1;
        PollResponse response = eventQueue.tryPoll( threshold, queueKey, clockKey );
        Assertions.assertNull( response.versionedEventTime() );
        Assertions.assertEquals( 0, response.queueSize() );
    }

    @Test
    public void tryPollReturnsOnlyListSizeWhenHeadYoungerThanThreshold() {
        int threshold = 1;
        EventTime event = new EventTime( "Test Event", 2 ); // notice later time than threshold
        eventQueue.offer( event, queueKey, clockKey );
        PollResponse response = eventQueue.tryPoll( threshold, queueKey, clockKey );
        Assertions.assertNull( response.versionedEventTime() );
        Assertions.assertEquals( 1, response.queueSize() );
    }

    @Test
    public void tryPollReturnsExpectedEventWhenHeadSameAgeAsThreshold() {
        int threshold = 1;
        EventTime event = new EventTime( "Test Event", threshold ); // notice same time as threshold
        eventQueue.offer( event, queueKey, clockKey );
        Versioned<EventTime> versionedEvent = eventQueue.tryPoll( threshold, queueKey, clockKey ).versionedEventTime();
        Assertions.assertEquals( event, versionedEvent.data(), "Event is expected" );
        Assertions.assertTrue( versionedEvent.clock() > 0, "clock > 0" );
    }

    @Test
    public void tryPollReturnsExpectedQueueSizeWhenResultAvailable() {
        int threshold = 1;
        EventTime event = new EventTime( "Test Event", threshold ); // notice same time as threshold
        eventQueue.offer( event, queueKey, clockKey );
        long expectedLength = 1;
        long foundLength = eventQueue.tryPoll( threshold, queueKey, clockKey ).queueSize();
        Assertions.assertEquals( expectedLength, foundLength );
    }

    @Test
    public void tryPollReturnsEventWhenHeadOlderThanThreshold() {
        int threshold = 1;
        EventTime event = new EventTime( "Test Event", 0 ); // notice older than threshold
        eventQueue.offer( event, queueKey, clockKey );
        Versioned<EventTime> versionedEvent = eventQueue.tryPoll( threshold, queueKey, clockKey ).versionedEventTime();
        Assertions.assertEquals( event, versionedEvent.data(), "Event is expected" );
        Assertions.assertTrue( versionedEvent.clock() > 0, "clock > 0" );
    }

    @Test
    public void tryPollDoesNotIncrementClockWhenNoElementPolled() {
        EventTime event = new EventTime( "Test Event", 1 );
        eventQueue.offer( event, queueKey, clockKey );
        long expectedClock = Long.parseLong( stringTemplate.opsForValue().get( clockKey ) );
        eventQueue.tryPoll( 0, queueKey, clockKey );
        long foundClock = Long.parseLong( stringTemplate.opsForValue().get( clockKey ) );
        Assertions.assertEquals( expectedClock, foundClock, "Clock should not change on a null poll." );
    }

    @Test
    public void getRangeEmptyQueue() {
        Assertions.assertEquals( List.of(), eventQueue.getRange( 0, -1, queueKey, clockKey ).data() );
    }

    @Test
    public void getAllNonEmptyQueue() {
        EventTime event0 = new EventTime( "Test Event0", 0 );
        EventTime event1 = new EventTime( "Test Event1", 1 );
        List<EventTime> expectedEvents = List.of( event0, event1 );
        expectedEvents.forEach( event -> eventQueue.offer( event, queueKey, clockKey ) );
        Versioned<List<EventTime>> versionedRange = eventQueue.getRange( 0, -1, queueKey, clockKey );
        Assertions.assertEquals( expectedEvents, versionedRange.data() );
    }

    @Test
    public void operationsAreLinearized() {
        // where clock starts is not generally enforced, so just referencing a start point.
        long expectedVersion = eventQueue.getRange( 0, -1, queueKey, clockKey ).clock();
        Assertions.assertEquals( ++expectedVersion,
                                 eventQueue.offer( new EventTime( "test", 0 ), queueKey, clockKey ).clock() );
        Assertions.assertEquals( ++expectedVersion,
                                 eventQueue.tryPoll( Instant.now().toEpochMilli(), queueKey, clockKey )
                                         .versionedEventTime().clock() );
        Assertions.assertEquals( ++expectedVersion, eventQueue.getRange( 0, -1, queueKey, clockKey ).clock() );
    }
}
