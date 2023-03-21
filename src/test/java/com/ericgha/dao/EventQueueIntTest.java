package com.ericgha.dao;

import com.ericgha.config.RedisConfig;
import com.ericgha.dto.EventTime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
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
// should not return null b/c not used in pipeline or transaction (see documentation)
import java.util.UUID;

@Testcontainers
@SpringBootTest(classes = {EventQueue.class, RedisConfig.class})
public class EventQueueIntTest {

    @Container
    private static final GenericContainer<?> redis = new GenericContainer<>( DockerImageName.parse( "redis:7" ) )
            .withExposedPorts( 6379 )
            .withReuse( true );

    @Autowired
    RedisConnectionFactory connectionFactory;
    @Autowired
    RedisTemplate<String, EventTime> eventTimeRedisTemplate;
    @Autowired
    EventQueue eventQueue;

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add( "spring.data.redis.host", redis::getHost );
        registry.add( "spring.data.redis.port", () -> redis.getMappedPort( 6379 ) );
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
    public void offerReturnsSizeOfQueue() {
        EventTime event = new EventTime( "first", 0 );
        Assertions.assertEquals( 1, eventQueue.offer( event ) );
        Assertions.assertEquals( 2, eventQueue.offer( event ) );
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
        Assertions.assertEquals( event, eventQueue.tryPoll( threshold ) );
    }

    @Test
    public void tryPollReturnsEventWhenHeadOlderThanThreshold() {
        int threshold = 1;
        EventTime event = new EventTime( "Test Event", 0 ); // notice older than threshold
        eventQueue.offer( event );
        Assertions.assertEquals( event, eventQueue.tryPoll( threshold ) );
    }


}
