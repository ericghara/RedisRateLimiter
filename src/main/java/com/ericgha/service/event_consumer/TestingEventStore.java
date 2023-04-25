package com.ericgha.service.event_consumer;

import com.ericgha.dto.EventTime;
import com.ericgha.dto.Versioned;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Simply stores events, and the time they were added to the {@code TestingEventStore}.  Intended for testing. Duplicate
 * events are not supported.
 */
public class TestingEventStore implements EventConsumer {

    private final Map<EventTime, Long> eventToVersion;
    private final Map<EventTime, Instant> eventToInstant;
    private Duration syntheticDelay;

    public TestingEventStore() {
        this( 0 );
    }

    public TestingEventStore(int syntheticDelayNanos) {
        this.eventToVersion = new ConcurrentHashMap<>();
        this.eventToInstant = new ConcurrentHashMap<>();
        setSyntheticDelay( syntheticDelayNanos );
    }

    public void setSyntheticDelay(int delayNanos) {
        syntheticDelay = Duration.of( delayNanos, ChronoUnit.NANOS );
    }

    public Duration getSyntheticDelay() {
        return this.syntheticDelay;
    }

    @Override
    public void accept(Versioned<EventTime> versionedEventTime) {
        if (Objects.isNull( versionedEventTime )) {
            throw new NullPointerException( "Received a null event." );
        }
        Long prevVersion = eventToVersion.putIfAbsent( versionedEventTime.data(), versionedEventTime.clock() );
        if (Objects.nonNull( prevVersion )) {
            throw new IllegalStateException(
                    String.format( "Event: %s was already seen. Current version %d, Previously seen version: %d.",
                                   versionedEventTime, versionedEventTime.clock(), prevVersion ) );
        }
        try {
            Thread.sleep( syntheticDelay.toMillis(), syntheticDelay.toNanosPart() % 1_000_000 );
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        eventToInstant.put( versionedEventTime.data(), Instant.now() );
    }

    public Map<EventTime, Long> eventsToVersions() {
        return Collections.unmodifiableMap( eventToVersion );
    }

    public Map<EventTime, Instant> eventsToInstantReceived() {
        return Collections.unmodifiableMap( eventToInstant );
    }

}
