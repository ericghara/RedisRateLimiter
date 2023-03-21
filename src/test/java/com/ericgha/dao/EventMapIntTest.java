package com.ericgha.dao;

import com.ericgha.config.RedisConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Instant;
import java.util.UUID;

@Testcontainers
@SpringBootTest(classes = {EventMap.class, RedisConfig.class})
public class EventMapIntTest {

    @Container
    private static final GenericContainer<?> redis = new GenericContainer<>( DockerImageName.parse( "redis:7" ) )
            .withExposedPorts( 6379 )
            .withReuse( true );
    @Autowired
    RedisConnectionFactory connectionFactory;
    @Autowired
    StringRedisTemplate template;
    @Autowired
    private EventMap eventMap;

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add( "spring.data.redis.host", redis::getHost );
        registry.add( "spring.data.redis.port", () -> redis.getMappedPort( 6379 ) );
        registry.add( "app.event-duration-millis", () -> 10_000 );
    }

    @AfterEach
    public void afterEach() {
        RedisConnection connection = connectionFactory.getConnection();
        connection.commands().flushAll();
    }

    @Test
    public void testPut() {
        String key = "Test Key";
        String expected = UUID.randomUUID().toString();
        eventMap.put( key, expected );
        Assertions.assertEquals( expected, eventMap.get( key ) );
    }

    @Test
    public void testGetNoKey() {
        String key = "Test Key";
        String found = eventMap.get( key );
        Assertions.assertNull( found );
    }

    @Test
    public void testPutEventNoDuplicate() {
        String event = "testEvent";
        boolean found = eventMap.putEvent( event, Instant.now().toEpochMilli() );
        Assertions.assertTrue( found );
    }

    @Test
    public void testPutEventBlockingDuplicate() {
        String event = "testEvent";
        boolean foundFirst = eventMap.putEvent( event, Instant.now().toEpochMilli() );
        boolean foundSecond = eventMap.putEvent( event, Instant.now().toEpochMilli() );
        Assertions.assertTrue( foundFirst );
        Assertions.assertFalse( foundSecond );
    }

    @Test
    public void testPutEventNonBlockingDuplicate(@Value("${app.event-duration-millis}") Long eventDuration) {
        String event = "testEvent";
        Long oldTime = Instant.now().toEpochMilli() - eventDuration;
        eventMap.put( event, oldTime.toString() );
        boolean didUpdate = eventMap.putEvent( event, Instant.now().toEpochMilli() );
        Assertions.assertTrue( didUpdate );
    }

    @Test
    public void deleteEventReturnsFalseOnAbsentEvent() {
        String event = "testEvent";
        long oldTime = Instant.now().toEpochMilli();
        boolean didDelete = eventMap.deleteEvent(event, oldTime);
        Assertions.assertFalse(didDelete);
    }

    @Test
    public void deleteEventDeletesWhenIsLatestEvent() {
        String event = "testEvent";
        long oldTime = Instant.now().toEpochMilli();
        eventMap.put(event, Long.toString(oldTime));
        boolean didDelete = eventMap.deleteEvent(event, oldTime);
        Assertions.assertTrue(didDelete, "expected return value");
        Assertions.assertNull(eventMap.get(event));
    }

    @Test
    public void deleteEventDeletesWhenEarlierEventInMap() {
        // note this is a condition that could indicate a problem, but making test to document behavior
        String event = "testEvent";
        long eventTime = Instant.now().toEpochMilli();
        eventMap.put(event, Long.toString(eventTime-1)); // earlier time
        boolean didDelete = eventMap.deleteEvent(event, eventTime);
        Assertions.assertTrue(didDelete, "expected return value");
        Assertions.assertNull( eventMap.get(event), "key is absent from DB" );
    }

    @Test
    public void deleteEventDoesNotDeleteWhenLaterEventInMap() {
        String event = "testEvent";
        long eventTime = Instant.now().toEpochMilli();
        eventMap.put(event, Long.toString(eventTime+1) ); // later time
        boolean didDelete = eventMap.deleteEvent(event, eventTime);
            Assertions.assertFalse(didDelete, "expected return value");
        Assertions.assertNotNull(eventMap.get(event));
    }

}