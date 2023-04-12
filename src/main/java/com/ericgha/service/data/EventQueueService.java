package com.ericgha.service.data;

import com.ericgha.dao.EventQueue;
import com.ericgha.dto.EventTime;
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

    public long offer(EventTime event) {
        return eventQueue.offer( event );
    }

    public long offer(String event, long time) {
        return this.offer( new EventTime( event, time ) );
    }

    /**
     * @param thresholdTime {@code time} of latest event that should be polled, younger objects will remain on queue
     * @return EventTime meeting {@code threholdTime} condition or {@code null}
     * @throws DirtyStateException if after retries still could not poll the queue (Likely causes contention or response
     *                             timeout)
     * @see RetryTemplate
     */
    @Nullable
    public EventTime tryPoll(long thresholdTime) throws DirtyStateException {
        return retryTemplate.execute( _context -> eventQueue.tryPoll( thresholdTime ) );
    }

    public List<EventTime> getRange(long start, long end) throws DirtyStateException {
        return retryTemplate.execute( _context -> eventQueue.getRange( start, end ) );
    }

    public List<EventTime> getAll() throws DirtyStateException {
        return this.getRange( 0, -1 );
    }

    public long size() {
        return retryTemplate.execute( _context -> eventQueue.size() );
    }

}
