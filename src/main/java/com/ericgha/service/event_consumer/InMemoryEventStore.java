package com.ericgha.service.event_consumer;

import com.ericgha.dto.EventTime;
import com.ericgha.dto.Versioned;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Simply stores events, and the time they were added to the {@code InMemoryEventStore}.  Intended for testing.
 * Duplicate events are not supported.
 */
public class InMemoryEventStore implements EventConsumer {

    private final Map<EventTime, Long> eventsByWallClock;
    private final Map<EventTime, Long> eventsByVersionClock;

    public InMemoryEventStore() {
        this.eventsByWallClock = new HashMap<>();
        this.eventsByVersionClock = new HashMap<>();
    }

    @Override
    public void accept(Versioned<EventTime> versionedEventTIme) {
        if (Objects.isNull( versionedEventTIme )) {
            throw new NullPointerException( "Received a null event." );
        }
        long curTime = Instant.now().toEpochMilli();
        Long prevTime = eventsByWallClock.putIfAbsent( versionedEventTIme.data(), Instant.now().toEpochMilli() );
        Long prevVersion = eventsByVersionClock.putIfAbsent( versionedEventTIme.data(), versionedEventTIme.clock()  );
        if (Objects.nonNull( prevTime ) ) {
            throw new IllegalStateException(
                    String.format( "Event: %s was already seen. Current Time %d, Previously added time: %d.",
                            versionedEventTIme, curTime, prevTime ) );
        }
    }

    public Map<EventTime, Long> eventsByWallClock() {
        return Collections.unmodifiableMap( eventsByWallClock );
    }

    public Map<EventTime, Long> eventsByVersionClock() {
        return Collections.unmodifiableMap( eventsByVersionClock );
    }

}
