package com.ericgha.service.data;

import com.ericgha.config.OnlyOnceEventConfig;
import com.ericgha.config.RedisConfig;
import com.ericgha.config.WebSocketConfig;
import com.ericgha.dto.EventTime;
import com.ericgha.service.event_consumer.InMemoryEventStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Instant;
import java.util.List;
import java.util.stream.IntStream;

@Testcontainers
@SpringBootTest(classes = {RedisConfig.class, OnlyOnceEventConfig.class, WebSocketConfig.class})
class EventExpiryServiceIntTest {

    @Container
    private static final GenericContainer<?> redis =
            new GenericContainer<>( DockerImageName.parse( "redis:7" ) ).withExposedPorts( 6379 ).withReuse( true );
    private static final int DELAY_MILLI = 10;  // delay period for queue
    @Autowired
    RedisConnectionFactory connectionFactory;
    @Autowired
    EventQueueService queueService;
    InMemoryEventStore eventStore = new InMemoryEventStore();
    EventExpiryService expiryService;

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add( "spring.data.redis.host", redis::getHost );
        registry.add( "spring.data.redis.port", () -> redis.getMappedPort( 6379 ) );
        registry.add( "app.event-duration-millis", () -> DELAY_MILLI ); // shouldn't matter for these tests
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
            Thread.sleep( 1, 500 );
            queueService.offer( Integer.toString( i ), Instant.now().toEpochMilli() );
        }
        Thread.sleep( 250 );
        for (int i = 20; i < 100; i++) {
            Thread.sleep( 1, 500_000 );
            queueService.offer( Integer.toString( i ), Instant.now().toEpochMilli() );
        }
        Thread.sleep( 300 );
        List<Integer> expectedEvents = IntStream.range( 0, 100 ).boxed().toList();
        List<Integer> foundEvents =
                eventStore.getAllEvents().keySet().stream().map( EventTime::event ).map( Integer::parseInt ).sorted()
                        .toList();
        Assertions.assertEquals( expectedEvents, foundEvents, "All events were polled" );
        long offBy100ms = eventStore.getAllEvents().entrySet().stream()
                .map( e -> e.getValue() - DELAY_MILLI - e.getKey().time() ).filter( d -> d > 100 ).count();
        Assertions.assertTrue( offBy100ms <= 3, "97% polled within 100ms of threshold delay" );
    }
}