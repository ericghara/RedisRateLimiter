package com.ericgha.dao;

import com.ericgha.config.RedisConfig;
import com.ericgha.dto.EventTime;
import com.ericgha.exception.DirtyStateException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
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
import java.util.concurrent.CompletableFuture;

@Testcontainers
@SpringBootTest(classes = {RedisConfig.class})
public class StrictlyOnceMapIntTest {

    @Container
    private static final GenericContainer<?> redis = new GenericContainer<>( DockerImageName.parse( "redis:7" ) )
            .withExposedPorts( 6379 )
            .withReuse( true );
    private static final int EVENT_DURATION = 10_000;
    private static final String RECENT_EVENTS_PREFIX = "RECENT_EVENTS";
    private static final String IS_VALID_PREFIX = "IS_VALID";
    @Autowired
    RedisTemplate<String, Long> stringLongRedisTemplate;
    @Autowired
    RedisConnectionFactory connectionFactory;
    StrictlyOnceMap strictlyOnceMap;

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add( "spring.data.redis.host", redis::getHost );
        registry.add( "spring.data.redis.port", () -> redis.getMappedPort( 6379 ) );
        registry.add( "spring.data.redis.password", () -> "" );
    }

    @BeforeEach
    void before() {
        this.strictlyOnceMap =
                new StrictlyOnceMap( EVENT_DURATION, RECENT_EVENTS_PREFIX, IS_VALID_PREFIX, stringLongRedisTemplate );
    }

    @AfterEach
    public void afterEach() {
        RedisConnection connection = connectionFactory.getConnection();
        connection.commands().flushAll();
    }


    @Test
    @DisplayName("Put with no recent events, adds to recentEvents and isValid")
    void putEventNoConflict() {
        EventTime eventTime = new EventTime( "Test 1", Instant.now().toEpochMilli() );
        Assertions.assertTrue( strictlyOnceMap.putEvent( eventTime ), "returns true, added to map" );
        Assertions.assertTrue( strictlyOnceMap.isValid( eventTime ), "eventTime is valid" );
        Assertions.assertEquals( eventTime.time(), strictlyOnceMap.getRecentEvent( eventTime.event() ),
                                 "recentEvents up to date" );
    }

    @Test
    @DisplayName(
            "Put duplicate events, first earlier, second later, no conflict, both validated recentEvents updated to later time")
    void putEventsFirstEarlierSecondLaterNoConflict() {
        EventTime firstEarlier = new EventTime( "Test 1", Instant.now().toEpochMilli() );
        strictlyOnceMap.putEvent( firstEarlier );
        EventTime secondLater = new EventTime( "Test 1", firstEarlier.time() + EVENT_DURATION );
        Assertions.assertTrue( strictlyOnceMap.putEvent( secondLater ), "putEvent returns true on no conflict" );
        Assertions.assertTrue( strictlyOnceMap.isValid( firstEarlier ), "earlier event isValid on no conflict" );
        Assertions.assertTrue( strictlyOnceMap.isValid( secondLater ), "later event isValid on no conflict" );
        Assertions.assertEquals( secondLater.time(), strictlyOnceMap.getRecentEvent( firstEarlier.event() ),
                                 "recent events updated to later event" );
    }

    @Test
    @DisplayName("Put 2 duplicate events with same time, updates recent events, invalidates old event")
    void putEqualTimeEventsHaveConflictTest() {
        EventTime eventTime = new EventTime( "Test 1", Instant.now().toEpochMilli() );
        strictlyOnceMap.putEvent( eventTime );
        Assertions.assertFalse( strictlyOnceMap.putEvent( eventTime ) );
        Assertions.assertFalse( strictlyOnceMap.isValid( eventTime ), "Existing eventTime invalidated" );
        Assertions.assertEquals( eventTime.time(), strictlyOnceMap.getRecentEvent( eventTime.event() ),
                                 "time expected" );
    }

    @Test
    @DisplayName(
            "Put 2 conflicting events, first earlier time second later time, updates recent events, invalidates old event")
    void putLaterEventHasConflictTest() {
        EventTime earlier = new EventTime( "Test 1", Instant.now().toEpochMilli() );
        EventTime later = new EventTime( "Test 1", Instant.now().toEpochMilli() + 1 );
        strictlyOnceMap.putEvent( earlier );
        Assertions.assertFalse( strictlyOnceMap.putEvent( later ) );
        Assertions.assertFalse( strictlyOnceMap.isValid( earlier ), "Existing eventTime invalidated" );
        Assertions.assertNull( strictlyOnceMap.isValid( later ), "new event not added to isValid" );
        Assertions.assertEquals( later.time(), strictlyOnceMap.getRecentEvent( later.event() ),
                                 "time updated to later" );
    }

    @Test
    @DisplayName(
            "Put 2 conflicting events, first later time second earlier time, no update recent events, invalidates old event")
    void putEventConflictWhenPuttingEarlierEvent() {
        EventTime earlier = new EventTime( "Test 1", Instant.now().toEpochMilli() );
        EventTime later = new EventTime( "Test 1", Instant.now().toEpochMilli() + 1 );
        strictlyOnceMap.putEvent( later ); // notice later put first
        Assertions.assertFalse( strictlyOnceMap.putEvent( earlier ) );
        Assertions.assertFalse( strictlyOnceMap.isValid( later ), "Existing eventTime invalidated" );
        Assertions.assertNull( strictlyOnceMap.isValid( earlier ), "rejected event not added to isValid" );
        Assertions.assertEquals( later.time(), strictlyOnceMap.getRecentEvent( later.event() ),
                                 "time remains later" );
    }

    @Test
    @DisplayName(
            "Duplicate events, second event earlier timestamp than first, no update recent events, no invalidation first event")
    void putEventTwoEventsSecondEarlierThanFirstNoConflict() {
        EventTime firstLater = new EventTime( "Test 1", Instant.now().toEpochMilli() );
        strictlyOnceMap.putEvent( firstLater );
        EventTime secondEarlier = new EventTime( firstLater.event(), firstLater.time() - EVENT_DURATION );
        Assertions.assertFalse( strictlyOnceMap.putEvent( secondEarlier ), "Cannot accept event in past." );
        Assertions.assertTrue( strictlyOnceMap.isValid( firstLater ), "No conflict on later event, remains valid" );
        Assertions.assertNull( strictlyOnceMap.isValid( secondEarlier ), "Earlier EventTime not added to isValid" );
        Assertions.assertEquals( firstLater.time(), strictlyOnceMap.getRecentEvent( firstLater.event() ),
                                 "Time remained later" );
    }

    @Test
    @DisplayName(
            "Duplicate events, first earlier, second later, has conflict, update recentEvents and invalid firstEvent")
    void putEventFirstEarlierSecondLaterHasConflict() {
        EventTime firstEarlier = new EventTime( "Test 1", Instant.now().toEpochMilli() );
        strictlyOnceMap.putEvent( firstEarlier );
        EventTime secondLater = new EventTime( "Test 1", firstEarlier.time() + 1L );
        Assertions.assertFalse( strictlyOnceMap.putEvent( secondLater ), "putEvent returns false on conflict" );
        Assertions.assertFalse( strictlyOnceMap.isValid( firstEarlier ), "Earlier event invalidated" );
        Assertions.assertNull( strictlyOnceMap.isValid( secondLater ),
                               "Later event not added to isValid b/c conflict" );
        Assertions.assertEquals( secondLater.time(), strictlyOnceMap.getRecentEvent( firstEarlier.event() ),
                                 "recentEvents updated to later event" );
    }

    @Test
    @DisplayName(
            "Duplicate events, first later, second earlier, has conflict, don't update recentEvents invalidate first")
    void putEventFirstLaterSecondEarlierHasConflict() {
        EventTime secondEarlier = new EventTime( "Test 1", Instant.now().toEpochMilli() );
        EventTime firstLater = new EventTime( secondEarlier.event(), secondEarlier.time() + 1L );
        strictlyOnceMap.putEvent( firstLater );
        Assertions.assertFalse( strictlyOnceMap.putEvent( secondEarlier ), "putEvent returns false on conflict" );
        Assertions.assertFalse( strictlyOnceMap.isValid( firstLater ), "first event invalidated on conflict" );
        Assertions.assertNull( strictlyOnceMap.isValid( secondEarlier ), "second event not added to map b/c conflict" );
        Assertions.assertEquals( firstLater.time(), strictlyOnceMap.getRecentEvent( firstLater.event() ),
                                 "recentEvents not updated by second earlier event" );
    }

    @Test
    @DisplayName("putEvent throws DirtyStateException on concurrent write")
    void putEventThrowsOnConcurrentWrite() {
        // Note: Non deterministic test
        long start = 0L;
        for (int repeat = 0; repeat < 5; repeat++, start += EVENT_DURATION * 2) {  // hacky way to retry failing test
            EventTime firstEarlier = new EventTime( "Test 1", start );
            EventTime secondLater = new EventTime( firstEarlier.event(), start + EVENT_DURATION );
            CompletableFuture<Void> otherWriter = null;
            try {
                otherWriter = CompletableFuture.runAsync( () -> {
                    for (; ; ) {
                        strictlyOnceMap.setIsValid( secondLater, false ); // putEvent watches this key
                    }
                } );
                Assertions.assertTrue( strictlyOnceMap.putEvent( firstEarlier ), "expect no conflict" );
                // other writer Writes to a key putEvent should be watching
                Assertions.assertTrue( strictlyOnceMap.putEvent( secondLater ), "expect no conflict" );
            } catch (DirtyStateException e) {  // we expect this, means a concurrent write occurred
                return;
            } finally {
                otherWriter.cancel( true );
            }
        }
        Assertions.fail( "Exhausted retries, No concurrent writes detected" );
    }
}
