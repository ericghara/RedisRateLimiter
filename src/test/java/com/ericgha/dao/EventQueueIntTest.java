package com.ericgha.dao;

import com.ericgha.config.RedisConfig;
import com.ericgha.dto.EventTime;
import com.ericgha.dto.Versioned;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Testcontainers
@SpringBootTest(classes = {RedisConfig.class})
public class EventQueueIntTest {

    @Container
    private static final GenericContainer<?> redis = new GenericContainer<>( DockerImageName.parse( "redis:7" ) )
            .withExposedPorts( 6379 )
            .withReuse( true );

    @Autowired
    RedisConnectionFactory connectionFactory;
    @Autowired
    RedisTemplate<String, EventTime> eventTimeRedisTemplate;
    EventQueue eventQueue;

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add( "spring.data.redis.host", redis::getHost );
        registry.add( "spring.data.redis.port", () -> redis.getMappedPort( 6379 ) );
    }

    @BeforeEach
    void before() {
        eventQueue = new EventQueue( eventTimeRedisTemplate );
    }

    @AfterEach
    public void afterEach() {
        RedisConnection connection = connectionFactory.getConnection();
        connection.commands().flushAll();
    }

    @Test
    public void queueIdReturnsUUIDWithAutowiredConstructor() {
        Assertions.assertDoesNotThrow( () -> UUID.fromString( eventQueue.queueId() ) );
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
