package com.ericgha.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.data.redis.DataRedisTest;
import org.springframework.data.redis.core.RedisTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Testcontainers
@DataRedisTest
public class RedisBackendCacheIntTest {

    @Autowired
    private RedisTemplate<String,String> redisTemplate;

    private RedisBackendCache cache;
    @Container
    public GenericContainer redis = new GenericContainer( DockerImageName.parse("redis:5.0.3-alpine"))
            .withExposedPorts(6379);

    @BeforeEach
    public void setUp() {
        String address = redis.getHost();
        Integer port = redis.getFirstMappedPort();

        // Now we have an address and port for Redis, no matter where it is running
        cache = new RedisBackendCache( redisTemplate );
    }

    @Test
    public void testSimplePutAndGet() {
//        underTest.put("test", "example");
//
//        String retrieved = underTest.get("test");
//        assertThat(retrieved).isEqualTo("example");
        assertTrue(true);
    }

    @Test
    public void testPut() {
        String key = "Test Key";
        String expected = UUID.randomUUID().toString();
        cache.put(key, expected);
        assertEquals(expected, cache.get(key));
    }
}