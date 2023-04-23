package com.ericgha.service.data;

import com.ericgha.dao.EventQueue;
import com.ericgha.dto.EventTime;
import com.ericgha.dto.Versioned;
import jakarta.annotation.Nullable;

import java.util.List;

public class EventQueueService {

    private final EventQueue eventQueue;

    public EventQueueService(EventQueue eventQueue) {
        this.eventQueue = eventQueue;
    }

    /**
     * @param event
     * @return current version
     * @throws IllegalArgumentException if any errors occur serializing the request
     * @throws IllegalStateException    if any errors occur deserializing the response
     */
    public long offer(EventTime event) throws IllegalArgumentException, IllegalStateException {
        return eventQueue.offer( event );
    }

    public long offer(String event, long time) throws IllegalArgumentException, IllegalStateException {
        return this.offer( new EventTime( event, time ) );
    }

    /**
     * @param thresholdTime {@code time} of latest event that should be polled, younger objects will remain on queue
     * @return EventTime meeting {@code thresholdTime} condition or {@code null}
     * @throws IllegalArgumentException if an error occurs deserializing the database response
     */
    @Nullable
    public Versioned<EventTime> tryPoll(long thresholdTime) throws IllegalArgumentException {
        return eventQueue.tryPoll( thresholdTime );
    }

    /**
     * @param start
     * @param end
     * @return
     * @throws IllegalStateException if an error occurs deserializing the database response
     */
    public Versioned<List<EventTime>> getRange(long start, long end) throws IllegalStateException {
        return eventQueue.getRange( start, end );
    }

    public Versioned<List<EventTime>> getAll() throws IllegalArgumentException {
        return this.getRange( 0, -1 );
    }

    public long size() {
        return eventQueue.size();
    }

    /**
     * Returns the key where the version clock value is stored.  This should be considered a restricted key for other
     * processes.
     *
     * @return clock key
     */
    public String clockKey() {
        return eventQueue.clockKey();
    }

    public String queueKey() {
        return eventQueue.queueId();
    }

}
