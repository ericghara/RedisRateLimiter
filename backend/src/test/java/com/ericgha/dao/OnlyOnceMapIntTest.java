package com.ericgha.dao;

import com.ericgha.service.data.FunctionRedisTemplate;
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
import redis.clients.jedis.Jedis;

import java.time.Instant;

@EnableRedisTestContainer
@SpringBootTest(classes = {RedisConfig.class})
public class OnlyOnceMapIntTest {


    private static final int EVENT_DURATION = 5; // ms
    @Autowired
    RedisConnectionFactory connectionFactory;
    @Autowired
    @Qualifier("stringTemplate")
    FunctionRedisTemplate<String, String> template;
    private OnlyOnceMap eventMap;

    @BeforeEach
    void before() {
        eventMap = new OnlyOnceMap( template );
    }

    @AfterEach
    public void afterEach() {
        try (RedisConnection connection = connectionFactory.getConnection() )  {
            connection.commands().flushAll();
        }
    }

    @Test
    public void putEventSetsKeyExpiryForNewEvent() {
        String key = "Test Key";
        long nowMilli = Instant.now().toEpochMilli();
        long expectedExpireTime = nowMilli + EVENT_DURATION;
        eventMap.putEvent( key, nowMilli, expectedExpireTime );
        long foundExpireTime;
        try (Jedis conn = template.getJedisConnection()) {
            foundExpireTime = conn.pexpireTime( key );
        }
        Assertions.assertEquals( expectedExpireTime, foundExpireTime );
    }

    @Test
    public void testPutEventNoDuplicate() {
        String event = "testEvent";
        boolean found = eventMap.putEvent( event, Instant.now().toEpochMilli(), EVENT_DURATION );
        Assertions.assertTrue( found );
    }

    @Test
    public void testPutEventBlockingDuplicate() throws InterruptedException {
        String event = "testEvent";
        long firstTime = Instant.now().toEpochMilli();
        eventMap.putEvent( event, firstTime, firstTime + EVENT_DURATION );
        Thread.sleep( 1 );
        long secondTime = Instant.now().toEpochMilli();
        boolean foundSecond = eventMap.putEvent( event, secondTime, secondTime + EVENT_DURATION );
        Assertions.assertFalse( foundSecond, "second put has conflict" );
    }

    @Test
    public void testPutEventNonBlockingDuplicate() throws InterruptedException {
        String event = "testEvent";
        long firstTime = Instant.now().toEpochMilli();
        eventMap.putEvent( event, firstTime, firstTime + EVENT_DURATION );
        Thread.sleep( EVENT_DURATION, 500_000 );
        long secondTime = Instant.now().toEpochMilli();
        boolean foundSecond = eventMap.putEvent( event, secondTime, secondTime + EVENT_DURATION );
        Assertions.assertTrue( foundSecond, "second put no conflict" );
    }

}