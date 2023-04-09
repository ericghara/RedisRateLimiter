package com.ericgha.service.data;

import com.ericgha.dao.EventMap;
import com.ericgha.dto.EventTime;
import exception.DirtyStateException;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.retry.support.RetryTemplate;

public class EventMapService {

    private final EventMap eventMap;
    private final RetryTemplate retryTemplate;

    public EventMapService(EventMap eventMap, @Qualifier("eventMapRetry") RetryTemplate retryTemplate) {
        // Qualifier only used for testing
        this.eventMap = eventMap;
        this.retryTemplate = retryTemplate;
    }

    public boolean tryAddEvent(String event, long timeMilli) throws DirtyStateException {
        return retryTemplate.execute( _context -> eventMap.putEvent( event, timeMilli ) );
    }

    public boolean tryAddEvent(EventTime eventTime) throws DirtyStateException {
        return tryAddEvent( eventTime.event(), eventTime.time() );
    }

    public boolean tryDeleteEvent(String event, long timeMilli) throws DirtyStateException {
        return retryTemplate.execute( _context -> eventMap.deleteEvent( event, timeMilli ) );
    }

    public boolean tryDeleteEvent(EventTime eventTime) throws DirtyStateException {
        return tryDeleteEvent( eventTime.event(), eventTime.time() );
    }
}
