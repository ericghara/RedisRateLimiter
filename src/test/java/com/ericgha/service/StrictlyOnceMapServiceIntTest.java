package com.ericgha.service;

import com.ericgha.service.data.FunctionRedisTemplate;
import com.ericgha.config.RedisConfig;
import com.ericgha.dao.StrictlyOnceMap;
import com.ericgha.domain.KeyMaker;
import com.ericgha.dto.EventTime;
import com.ericgha.service.data.StrictlyOnceMapService;
import com.ericgha.test_fixtures.EnableRedisTestContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import redis.clients.jedis.Jedis;

/*
StrictlyOnceMapService was extensively unit tested.  This is a quick smoke test to make sure the Service/DAO
interface was correctly modeled by mocks.
 */
@EnableRedisTestContainer
@SpringBootTest(classes=RedisConfig.class)
public class StrictlyOnceMapServiceIntTest {

    final long EVENT_DURATION = 10_000L;
    final String KEY_PREFIX = "STRICTLY_ONCE_TEST";

    KeyMaker keyMaker = new KeyMaker( KEY_PREFIX);

    @Autowired
    @Qualifier("stringLongTemplate")
    FunctionRedisTemplate<String, Long> redisTemplate;

    StrictlyOnceMap eventMap;
    StrictlyOnceMapService eventMapService;

    @BeforeEach
    void before() {
        this.eventMap = new StrictlyOnceMap(redisTemplate);
        this.eventMapService = new StrictlyOnceMapService( eventMap, EVENT_DURATION, keyMaker );
    }

    @AfterEach
    public void afterEach() {
        try(Jedis connection = redisTemplate.getJedisConnection()) {
            connection.flushAll();
        }
    }

    @Test
    @DisplayName( "put into empty map returns true" )
    void putIntoEmptyMapReturnsTrue() {
        EventTime eventTime = new EventTime("Test 1", 1L);
        Assertions.assertTrue( eventMapService.putEvent( eventTime ) );
    }

    @Test
    @DisplayName( "putEvent returns false when new event conflicts with a previously added event" )
    void putEventReturnsFalseOnConflict() {
        EventTime firstLater = new EventTime("Test 1", 1L);
        eventMapService.putEvent(firstLater);
        EventTime secondEarlier = new EventTime(firstLater.event(), 0L);
        Assertions.assertFalse(eventMapService.putEvent(secondEarlier));
    }

    @Test
    @DisplayName("putEvent returns true when new event does not conflict with a previously added event")
    void putEventReturnsTrueWhenNoConflictWithPreviousEvent() {
        EventTime firstEarlier = new EventTime("Test 1", 0L);
        eventMapService.putEvent(firstEarlier);
        EventTime secondLater = new EventTime("Test 1", EVENT_DURATION);
        Assertions.assertTrue(eventMapService.putEvent(secondLater));
    }
}
