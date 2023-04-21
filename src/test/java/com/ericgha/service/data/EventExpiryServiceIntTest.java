package com.ericgha.service.data;

import com.ericgha.config.FunctionRedisTemplate;
import com.ericgha.config.OnlyOnceEventConfig;
import com.ericgha.config.RedisConfig;
import com.ericgha.config.WebSocketConfig;
import com.ericgha.dto.EventTime;
import com.ericgha.service.event_consumer.InMemoryEventStore;
import com.ericgha.test_fixtures.EnableRedisTestContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

@EnableRedisTestContainer
@SpringBootTest(classes = {RedisConfig.class, OnlyOnceEventConfig.class, WebSocketConfig.class})
class EventExpiryServiceIntTest {

    private static final int DELAY_MILLI = 10;  // delay period for queue
    @Autowired
    RedisConnectionFactory connectionFactory;
    @Autowired
    @Qualifier("stringTemplate")
    FunctionRedisTemplate<String, String> stringTemplate;

    @Autowired
    @Qualifier("onlyOnceEventQueueService")
    EventQueueService queueService;
    InMemoryEventStore eventStore = new InMemoryEventStore();
    EventExpiryService expiryService;

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add( "app.only-once-event.event-duration-millis",
                      () -> DELAY_MILLI ); // PropertyRegistry shouldn't be read by these tests
        // disable beans
        registry.add( "app.only-once-event.disable-bean.event-expiry-service", () -> true );
        registry.add( "app.only-once-event.disable-bean.event-queue-snapshot-service", () -> true );
    }

    @BeforeEach
    void before() {
        expiryService = new EventExpiryService( queueService );
        expiryService.start( eventStore, DELAY_MILLI, 2 );
    }

    @AfterEach
    void after() {
        expiryService.stop();
        RedisConnection connection = connectionFactory.getConnection();
        connection.commands().flushAll();
    }

    @Test
    @Timeout(2)
    void pollsEvents() throws InterruptedException {
        for (int i = 0; i < 20; i++) {
            Thread.sleep( 1, 500 ); // sleeps to reduce contention, expiry service is working concurrently
            queueService.offer( Integer.toString( i ), Instant.now().toEpochMilli() );
        }
        Thread.sleep( 250 );
        for (int i = 20; i < 100; i++) {
            Thread.sleep( 1, 500_000 );
            queueService.offer( Integer.toString( i ), Instant.now().toEpochMilli() );
        }
        while (queueService.size() > 0) {
            Thread.sleep( 1 );
        }
        List<Integer> expectedEvents = IntStream.range( 0, 100 ).boxed().toList();
        List<Integer> foundEvents =
                eventStore.eventsByWallClock().keySet().stream().map( EventTime::event ).map( Integer::parseInt )
                        .sorted()
                        .toList();
        Assertions.assertEquals( expectedEvents, foundEvents, "All events were polled" );
        long offBy100ms = eventStore.eventsByWallClock().entrySet().stream()
                .map( e -> e.getValue() - DELAY_MILLI - e.getKey().time() ).filter( d -> d > 100 ).count();
        Assertions.assertTrue( offBy100ms <= 3, "97% polled within 100ms of threshold delay" );
    }

    @Test
    @DisplayName("EventExpiryService messages with expected versions")
    @Timeout(value = 500, unit = TimeUnit.MILLISECONDS)
    void messagesWithExpectedVersions() throws InterruptedException {
        stringTemplate.opsForValue().set( queueService.clockKey(), "0" ); // set to 0 to avoid handling null key
        EventTime eventTime = new EventTime( "test", 0 );
        queueService.offer( eventTime );
        while (eventStore.eventsByVersionClock().isEmpty()) { // spin
            Thread.sleep( 1 );
        }
        Map<EventTime, Long> publishedEvents = eventStore.eventsByVersionClock();
        Assertions.assertEquals( 1, publishedEvents.size(), "One event was published" );
        Assertions.assertEquals( 2, publishedEvents.get( eventTime ), "Version clock is 2" );
    }
}