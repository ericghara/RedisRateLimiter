package com.ericgha.service.data;

import com.ericgha.config.DaoConfig;
import com.ericgha.config.OnlyOnceEventConfig;
import com.ericgha.config.RedisConfig;
import com.ericgha.config.WebSocketConfig;
import com.ericgha.dto.EventTime;
import com.ericgha.service.event_consumer.TestingEventStore;
import com.ericgha.test_fixtures.EnableRedisTestContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

@EnableRedisTestContainer
@SpringBootTest(classes = {RedisConfig.class, DaoConfig.class, OnlyOnceEventConfig.class, WebSocketConfig.class})
class EventExpiryServiceIntTest {

    private static final int DELAY_MILLI = 10;  // delay period for queue
    private static final int NUM_WORKERS = 2;
    @Autowired
    RedisConnectionFactory connectionFactory;
    @Autowired
    @Qualifier("stringTemplate")
    FunctionRedisTemplate<String, String> stringTemplate;

    @Autowired
    @Qualifier("onlyOnceEventQueueService")
    EventQueueService queueService;
    TestingEventStore eventStore = new TestingEventStore();
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
        expiryService.start( eventStore, DELAY_MILLI, NUM_WORKERS );
    }

    @AfterEach
    void after() {
        expiryService.stop();
        try (RedisConnection connection = connectionFactory.getConnection()) {
            connection.commands().flushAll();
        }
    }

    @Test
    @DisplayName("workers poll all events within 0.5 sec.")
    @Timeout(value = 500, unit = TimeUnit.MILLISECONDS)
    void pollsAllEvents() throws InterruptedException {
        for (int i = 0; i < 100; i++) {
            queueService.offer( Integer.toString( i ), Instant.now().toEpochMilli() );
        }
        while (queueService.size() > 0) {
            Thread.sleep( 1 );
        }
        //test completed before timeout.
    }

    @Test
    @DisplayName("90% of Events are polled within 10 ms of event duration.")
    @Disabled("This test was useful for characterizing performance during development, but is a bad general test.")
    void pollsEventsPromptly() throws InterruptedException {
        final int NUM_EVENTS = 100;
        final int MAX_LATENCY = 10; //ms
        for (int i = 0; i < 20; i++) {
            queueService.offer( Integer.toString( i ), Instant.now().toEpochMilli() );
        }
        Thread.sleep( 250 );
        for (int i = 20; i < 100; i++) {
            queueService.offer( Integer.toString( i ), Instant.now().toEpochMilli() );
        }
        while (eventStore.eventsToVersions().size() < NUM_EVENTS) {
            Thread.sleep( 1 );
        }
        double numFail = eventStore.eventsToInstantReceived().entrySet().stream()
                .map( e -> e.getValue().toEpochMilli() - DELAY_MILLI - e.getKey().time() )
                .filter( d -> d > MAX_LATENCY )
                .count();
        double percentPass = ( NUM_EVENTS - numFail ) / NUM_EVENTS * 100;
        System.out.printf( "%.0f%% of events were polled within %d ms.", percentPass, MAX_LATENCY );
        Assertions.assertTrue( percentPass >= 90, "90% polled within " + MAX_LATENCY + " ms" );
    }

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(useHeadersInDisplayName = true, textBlock = """
            num_workers, time_to_poll_ms
            1,              225
            2,              100
            4,              60
            8,              30
            """)
    @Timeout(3)
    @DisplayName("Rate of polling decreasing as number of workers increasing (i.e. it scales). ")
    @Disabled(
            "Dependant on hardware it's run on, was useful for verifying efficient parallelization, but a bad general test.")
    void pollThroughputScalesWithNumWorkers(int numWorkers, int timeToPollMs) throws InterruptedException {
        final int NUM_EVENTS = 100;  // num events to poll
        final int SYNTHETIC_DELAY_NANO = 1_000_000; // 'work' each worker must complete before moving to next event
        // stop and add all events
        expiryService.stop();
        IntStream.range( 0, NUM_EVENTS ).mapToObj( i -> new EventTime( Integer.toString( i ), i ) )
                .forEach( queueService::offer );
        eventStore.setSyntheticDelay( SYNTHETIC_DELAY_NANO );
        // Start! All events far in past so should be polled immediately (delayMilli doesn't matter)
        expiryService.start( eventStore, 10_000, numWorkers );
        Instant start = Instant.now();
        while (eventStore.eventsToVersions().size() < NUM_EVENTS) {  // spin until all polled
            Thread.sleep( 1 );
        }
        long pollTime = start.until( Instant.now(), ChronoUnit.MILLIS );
        System.out.printf( "Polled %d events in %d ms.", NUM_EVENTS, pollTime );

        Assertions.assertTrue( pollTime < timeToPollMs );
    }


    @Test
    @DisplayName("EventExpiryService messages with expected versions")
    @Timeout(value = 500, unit = TimeUnit.MILLISECONDS)
    void messagesWithExpectedVersions() throws InterruptedException {
        stringTemplate.opsForValue().set( queueService.clockKey(), "0" ); // set to 0 to avoid handling null key
        EventTime eventTime = new EventTime( "test", 0 );
        queueService.offer( eventTime );
        while (eventStore.eventsToVersions().isEmpty()) { // spin
            Thread.sleep( 1 );
        }
        Map<EventTime, Long> publishedEvents = eventStore.eventsToVersions();
        Assertions.assertEquals( 1, publishedEvents.size(), "One event was published" );
        Assertions.assertEquals( 2, publishedEvents.get( eventTime ), "Version clock is 2" );
    }
}