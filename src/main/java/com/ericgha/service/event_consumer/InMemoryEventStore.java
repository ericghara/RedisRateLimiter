package com.ericgha.service.event_consumer;

import com.ericgha.dto.EventTime;

import java.time.Instant;
import java.util.Collections;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Simply stores events, and the time they were added to the {@code InMemoryEventStore}.  Intended for testing.
 * Duplicate events are not supported.
 */
public class InMemoryEventStore implements EventConsumer {

    private final TreeMap<EventTime, Long> eventsByTimeAdded;

    public InMemoryEventStore() {
        this.eventsByTimeAdded = new TreeMap<>( EventTime.descTimeDescEventComparator() );
    }

    @Override
    public void accept(EventTime eventTime) {
        if (Objects.isNull( eventTime )) {
            throw new NullPointerException( "Received a null event." );
        }
        long curTime = Instant.now().toEpochMilli();
        Long prevTime = eventsByTimeAdded.putIfAbsent( eventTime, Instant.now().toEpochMilli() );
        if (Objects.nonNull( prevTime )) {
            throw new IllegalStateException(
                    String.format( "Event: %s was already seen. Current Time %d, Previously added time: %d.",
                            eventTime, curTime, prevTime ) );
        }
    }

    public SortedMap<EventTime, Long> getAllEvents() {
        return Collections.unmodifiableSortedMap( eventsByTimeAdded );
    }
}
