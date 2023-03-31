package com.ericgha.service;

import com.ericgha.dao.EventQueue;
import com.ericgha.dto.EventTime;
import exception.DirtyStateException;
import jakarta.annotation.Nullable;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Service;

@Service
public class EventQueueService {

    private final EventQueue eventQueue;
    private final RetryTemplate retryTemplate;

    public EventQueueService(EventQueue eventQueue, @Qualifier("eventQueueRetryTemplate") RetryTemplate retryTemplate) {
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
     *
     * @param thresholdTime {@code time} of latest event that should be polled, younger objects will remain on queue
     * @return EventTime meeting {@code threholdTime} condition or {@code null}
     * @throws DirtyStateException if after retries still could not poll the queue (Likely causes contention or response timeout)
     * @see RetryTemplate
     */
    @Nullable
    public EventTime tryPoll(long thresholdTime) throws DirtyStateException {
        return retryTemplate.execute( _context -> eventQueue.tryPoll( thresholdTime ) );
    }

}
