package com.ericgha.service.data;

import com.ericgha.dao.EventQueue;
import com.ericgha.domain.KeyMaker;
import com.ericgha.dto.EventTime;
import com.ericgha.dto.PollResponse;
import com.ericgha.dto.Versioned;
import jakarta.annotation.Nullable;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

public class EventQueueService {

    public final long LAST_SIZE_TTL_MILLI = 50;

    private final EventQueue eventQueue;
    private final String queueKey;
    private final String clockKey;

    volatile private Long lastSize;
    volatile private Instant lastSizeTimestamp;


    public EventQueueService(EventQueue eventQueue, KeyMaker keyMaker) {
        this.eventQueue = eventQueue;
        this.lastSize = 0L;
        this.lastSizeTimestamp = Instant.EPOCH;
        this.queueKey = keyMaker.generateQueueKey();
        this.clockKey = keyMaker.generateClockKey();
    }

    /**
     * Offers event and updates lastSize
     * @param event
     * @return current version
     * @throws IllegalArgumentException if any errors occur serializing the request
     * @throws IllegalStateException    if any errors occur deserializing the response
     */
    public long offer(EventTime event) throws IllegalArgumentException, IllegalStateException {
        Versioned<Long> versionedSize = eventQueue.offer( event, queueKey, clockKey );
        updateSize(versionedSize.data());
        return versionedSize.clock();
    }

    public long offer(String event, long time) throws IllegalArgumentException, IllegalStateException {
        return this.offer( new EventTime( event, time ) );
    }

    /**
     * Polls if the event at the head of the queue older than threshold time.  If not event is meets threshold criteria,
     * or the queue is empty, returns null.  Always updates {@code lastSize}.
     * @param thresholdTime {@code time} of latest event that should be polled, younger objects will remain on queue
     * @return EventTime meeting {@code thresholdTime} condition or {@code null}
     * @throws IllegalArgumentException if an error occurs deserializing the database response
     */
    @Nullable
    public Versioned<EventTime> tryPoll(long thresholdTime) throws IllegalArgumentException {
        PollResponse pollResponse = eventQueue.tryPoll( thresholdTime, queueKey, clockKey );
        updateSize( pollResponse.queueSize() );
        return pollResponse.versionedEventTime();
    }

    /**
     * @param start
     * @param end
     * @return
     * @throws IllegalStateException if an error occurs deserializing the database response
     */
    public Versioned<List<EventTime>> getRange(long start, long end) throws IllegalStateException {
        return eventQueue.getRange( start, end, queueKey, clockKey );
    }

    public Versioned<List<EventTime>> getAll() throws IllegalArgumentException {
        return this.getRange( 0, -1 );
    }

    /**
     * updates {@code lastSize}.
     *
     * @return current queue size.  Calls database
     */
    public long size() {
        long currentSize = eventQueue.size(queueKey);
        updateSize( currentSize );
        return currentSize;
    }

    /**
     * For cases when the exact queue size is not required.  The size returned was guaranteed to be read within
     * the last {@code LAST_SIZE_TTL_MILLI}.  If the last length read is older, then a call to the database will
     * be made to get the current queue size;
     * @return
     */
    public long approxSize() {
        // no effort to prevent dirty reads, the penalty of a stale read is < 1ms.
        if (lastSizeTimestamp.until( Instant.now(), ChronoUnit.MILLIS) <= LAST_SIZE_TTL_MILLI) {
            return lastSize;
        }
        return size();
    }

    /**
     * Returns the key where the version clock value is stored.  This should be considered a restricted key for other
     * processes.
     *
     * @return clock key
     */
    public String clockKey() {
        return this.clockKey;
    }

    public String queueKey() {
        return this.queueKey;
    }

    // no effort to make atomic updates, we expect and accept uncommitted reads
    private void updateSize(long currentSize) {
        this.lastSizeTimestamp = Instant.now();
        this.lastSize = currentSize;
    }

}
