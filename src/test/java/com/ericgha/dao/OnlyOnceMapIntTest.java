package com.ericgha.dao;

import com.ericgha.config.FunctionRedisTemplate;
import com.ericgha.config.RedisConfig;
import com.ericgha.test_fixtures.EnableRedisTestContainer;
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
import java.util.concurrent.TimeUnit;

@EnableRedisTestContainer
@SpringBootTest(classes = {RedisConfig.class})
public class OnlyOnceMapIntTest {


    private static final int EVENT_DURATION = 10_000;
    @Autowired
    RedisConnectionFactory connectionFactory;
    @Autowired
    @Qualifier("stringTemplate")
    FunctionRedisTemplate<String, String> template;
    private OnlyOnceMap eventMap;

    @BeforeEach
    void before() {
        eventMap = new OnlyOnceMap( template);
    }

    @AfterEach
    public void afterEach() {
        RedisConnection connection = connectionFactory.getConnection();
        connection.commands().flushAll();
    }

    @Test
    public void putEventSetsKeyExpiryForNewEvent() {
        String key = "Test Key";
        long nowMilli = Instant.now().toEpochMilli();
        eventMap.putEvent( key, nowMilli, EVENT_DURATION );
        long expireMilli = template.getExpire( key, TimeUnit.MILLISECONDS );
        Assertions.assertTrue(expireMilli <= EVENT_DURATION, "expire not > than expected");
        // 50 ms is generous, should take only a couple ms to getExp
        Assertions.assertTrue( expireMilli >=EVENT_DURATION - 50, "Expire within 50 ms of expected");
    }

    @Test
    public void putEventSetsKeyExpiryForValidSubsiquentEvent() {
        String key = "Test Key";
        long nowMilli = Instant.now().toEpochMilli();
        eventMap.put(key, Long.toString(nowMilli) );
        long futureTime = nowMilli+EVENT_DURATION;
        eventMap.putEvent(key, futureTime, EVENT_DURATION);
        long expireMilli = template.getExpire( key, TimeUnit.MILLISECONDS );
        Assertions.assertTrue(expireMilli <= EVENT_DURATION, "expire not > than expected");
        // 50 ms is generous, should take only a couple ms to getExp
        Assertions.assertTrue( expireMilli >=EVENT_DURATION - 50, "Expire within 50 ms of expected");
    }

    @Test
    public void testGetNoKey() {
        String key = "Test Key";
        Long found = eventMap.get( key );
        Assertions.assertNull( found );
    }

    @Test
    public void testPutEventNoDuplicate() {
        String event = "testEvent";
        boolean found = eventMap.putEvent( event, Instant.now().toEpochMilli(), EVENT_DURATION );
        Assertions.assertTrue( found );
    }

    @Test
    public void testPutEventBlockingDuplicate() {
        String event = "testEvent";
        boolean foundFirst = eventMap.putEvent( event, Instant.now().toEpochMilli(), EVENT_DURATION );
        boolean foundSecond = eventMap.putEvent( event, Instant.now().toEpochMilli(), EVENT_DURATION );
        Assertions.assertTrue( foundFirst );
        Assertions.assertFalse( foundSecond );
    }

    @Test
    public void testPutEventNonBlockingDuplicate() {
        String event = "testEvent";
        Long oldTime = Instant.now().toEpochMilli() - EVENT_DURATION;
        eventMap.put( event, oldTime.toString() );
        boolean didUpdate = eventMap.putEvent( event, Instant.now().toEpochMilli(), EVENT_DURATION );
        Assertions.assertTrue( didUpdate );
    }

    @Test
    public void deleteEventThrowsWhenEventTooYoung() {
        String event = "testEvent";
        long oldTime = Instant.now().toEpochMilli() - EVENT_DURATION + 5; // event 5 ms too young
        Assertions.assertThrows( IllegalArgumentException.class, () -> eventMap.deleteEvent( event, oldTime, EVENT_DURATION ) );
    }

    @Test
    public void deleteEventReturnsFalseOnAbsentEvent() {
        String event = "testEvent";
        long oldTime = Instant.now().toEpochMilli() - EVENT_DURATION;
        boolean didDelete = eventMap.deleteEvent( event, oldTime, EVENT_DURATION );
        Assertions.assertFalse( didDelete );
    }

    @Test
    public void deleteEventDeletesWhenIsLatestEvent() {
        String event = "testEvent";
        long oldTime = Instant.now().toEpochMilli() - EVENT_DURATION;
        eventMap.put( event, Long.toString( oldTime ) );
        boolean didDelete = eventMap.deleteEvent( event, oldTime, EVENT_DURATION );
        Assertions.assertTrue( didDelete, "expected return value" );
        Assertions.assertNull( eventMap.get( event ) );
    }

    @Test
    public void deleteEventDeletesWhenEarlierEventInMap() {
        // note this is a condition that could indicate a problem, but making test to document behavior
        String event = "testEvent";
        long eventTime = Instant.now().toEpochMilli() - EVENT_DURATION;
        eventMap.put( event, Long.toString( eventTime - 1 ) ); // earlier time
        boolean didDelete = eventMap.deleteEvent( event, eventTime, EVENT_DURATION );
        Assertions.assertTrue( didDelete, "expected return value" );
        Assertions.assertNull( eventMap.get( event ), "key is absent from DB" );
    }

    @Test
    public void deleteEventDoesNotDeleteWhenLaterEventInMap() {
        String event = "testEvent";
        long eventTime = Instant.now().toEpochMilli() - EVENT_DURATION;
        eventMap.put( event, Long.toString( eventTime + 5 ) ); // later time
        boolean didDelete = eventMap.deleteEvent( event, eventTime, EVENT_DURATION );
        Assertions.assertFalse( didDelete, "expected return value" );
        Assertions.assertNotNull( eventMap.get( event ) );
    }

}