package com.ericgha.service;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.data.redis.DataRedisTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.UUID;

@Testcontainers
@DataRedisTest(includeFilters = @ComponentScan.Filter(type = FilterType.ASSIGNABLE_TYPE, classes = EventMap.class))
public class EventMapIntTest {

    @Container
    private static final GenericContainer<?> redis = new GenericContainer<>( DockerImageName.parse("redis:5.0.3-alpine"))
            .withExposedPorts(6379);

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.redis.host", redis::getHost);
        registry.add("spring.data.redis.port", () -> redis.getMappedPort(6379));
        registry.add("app.event-duration-millis", () -> 50_000_000);
    }

    @Autowired
    private EventMap eventMap;
    @Autowired
    RedisConnectionFactory connectionFactory;

    @AfterEach
    public void afterEach() {
        connectionFactory.getConnection().commands().flushAll();
    }

    @Test
    public void testPut() {
        String key = "Test Key";
        String expected = UUID.randomUUID().toString();
        eventMap.put(key, expected);
        Assertions.assertEquals( expected, eventMap.get(key) );
    }

    @Test
    public void testGetNoKey() {
        String key = "Test Key";
        String found = eventMap.get(key);
        Assertions.assertNull(found);
    }

    @Test
    public void testPutEventNoDuplicate() {
        String event = "testEvent";
        boolean found = eventMap.putEvent(event);
        Assertions.assertTrue(found);
    }

    @Test
    public void testPutEventBlockingDuplicate() {
        String event = "testEvent";
        boolean foundFirst = eventMap.putEvent(event);
        boolean foundSecond = eventMap.putEvent(event);
        Assertions.assertTrue(foundFirst);
        Assertions.assertFalse(foundSecond);
    }
}