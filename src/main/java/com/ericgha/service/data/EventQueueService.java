package com.ericgha.service.data;

import com.ericgha.dao.EventQueue;
import com.ericgha.dto.EventTime;
import com.ericgha.dto.Versioned;
import com.ericgha.exception.DirtyStateException;
import jakarta.annotation.Nullable;
import org.springframework.retry.support.RetryTemplate;

import java.util.List;

public class EventQueueService {

    private final EventQueue eventQueue;
    private final RetryTemplate retryTemplate;

    public EventQueueService(EventQueue eventQueue, RetryTemplate retryTemplate) {
        this.eventQueue = eventQueue;
        this.retryTemplate = retryTemplate;
    }

    /**
     *
     * @param event
     * @return current version
     * @throws DirtyStateException if the version clock was concurrently modified
     */
    public long offer(EventTime event) throws DirtyStateException {
        return retryTemplate.execute( _context -> eventQueue.offer( event ) );
    }

    public long offer(String event, long time) throws DirtyStateException {
        return this.offer( new EventTime( event, time ) );
    }

    /**
     * @param thresholdTime {@code time} of latest event that should be polled, younger objects will remain on queue
     * @return EventTime meeting {@code thresholdTime} condition or {@code null}
     * @throws DirtyStateException if after retries still could not poll the queue (Likely causes contention or response
     *                             timeout)
     * @see RetryTemplate
     */
    @Nullable
    public Versioned<EventTime> tryPoll(long thresholdTime) throws DirtyStateException {
        return retryTemplate.execute( _context -> eventQueue.tryPoll( thresholdTime ) );
    }

    public Versioned<List<EventTime>> getRange(long start, long end) throws DirtyStateException {
        return retryTemplate.execute( _context -> eventQueue.getRange( start, end ) );
    }

    public Versioned<List<EventTime>> getAll() throws DirtyStateException {
        return this.getRange( 0, -1 );
    }

    public long size() {
        return retryTemplate.execute( _context -> eventQueue.size() );
    }

    /**
     * Returns the key where the version clock value is stored.  This should be considered a restricted
     * key for other processes.
     * @return clock key
     */
    public String clockKey() {
        return eventQueue.clockKey();
    }

}
