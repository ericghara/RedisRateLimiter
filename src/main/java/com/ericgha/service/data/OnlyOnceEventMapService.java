package com.ericgha.service.data;

import com.ericgha.dao.OnlyOnceMap;
import com.ericgha.domain.KeyMaker;
import com.ericgha.dto.EventTime;
import com.ericgha.exception.DirtyStateException;
import org.springframework.retry.support.RetryTemplate;

/**
 * Event map DAO is not prefix aware.  Injects prefix to multiplex multiple EventMaps on one DB instance.
 * <p>
 * <em>Note: </em> Decided to take multiplex approach over using multiple DBs as this is a much lighter
 * and does not require multiple connection factories.
 */
public class OnlyOnceEventMapService implements EventMapService {

    private final OnlyOnceMap eventMap;
    private final KeyMaker keyMaker;
    private final RetryTemplate retryTemplate;
    private final long eventDurationMilli;

    public OnlyOnceEventMapService(OnlyOnceMap eventMap, long eventDurationMilli, KeyMaker keyMaker, RetryTemplate retryTemplate) {
        // Qualifier only used for testing
        this.eventDurationMilli = validateEventDuration( eventDurationMilli );
        this.eventMap = eventMap;
        this.keyMaker = keyMaker;
        this.retryTemplate = retryTemplate;
    }

    public boolean putEvent(String event, long timeMilli) throws DirtyStateException {
        String key = keyMaker.generateEventKey( event );
        return retryTemplate.execute( _context -> eventMap.putEvent( key, timeMilli, eventDurationMilli ) );
    }

    public boolean putEvent(EventTime eventTime) throws DirtyStateException {
        return putEvent( eventTime.event(), eventTime.time() );
    }

    public boolean tryDeleteEvent(String event, long timeMilli) throws DirtyStateException {
        String key = keyMaker.generateEventKey( event );
        return retryTemplate.execute( _context -> eventMap.deleteEvent( key, timeMilli, eventDurationMilli ) );
    }

    public boolean tryDeleteEvent(EventTime eventTime) throws DirtyStateException {
        return tryDeleteEvent( eventTime.event(), eventTime.time() );
    }

    public String keyPrefix() {
        return keyMaker.keyPrefix();
    }

    public long validateEventDuration(long millis) throws IllegalArgumentException {
        if (millis < 0) {
            throw new IllegalArgumentException( "Event duration must be a positive long" );
        }
        return millis;
    }
}
