package com.ericgha.service.data;

import com.ericgha.dao.EventMap;
import com.ericgha.dto.EventTime;
import exception.DirtyStateException;
import org.springframework.retry.support.RetryTemplate;

/**
 * Event map DAO is not prefix aware.  Injects prefix to multiplex multiple EventMaps on one DB instance.
 * <p>
 * <em>Note: </em> Decided to take multiplex approach over using multiple DBs as this is a much lighter
 * and does not require multiple connection factories.
 */
public class EventMapService {

    //  delimiter b/t prefix and key; i.e {PREFIX}.{KEY} for ELEMENT_SEPARATOR = '.'
    private static final char ELEMENT_DELIMITER = '.';
    private final EventMap eventMap;
    private final String keyPrefix;
    private final RetryTemplate retryTemplate;

    public EventMapService(EventMap eventMap, String keyPrefix, RetryTemplate retryTemplate) {
        // Qualifier only used for testing
        this.eventMap = eventMap;
        this.keyPrefix = keyPrefix;
        this.retryTemplate = retryTemplate;
    }

    private String generateKey(String event) {
        return keyPrefix + ELEMENT_DELIMITER + event;
    }

    public boolean tryAddEvent(String event, long timeMilli) throws DirtyStateException {
        String key = generateKey( event );
        return retryTemplate.execute( _context -> eventMap.putEvent( key, timeMilli ) );
    }

    public boolean tryAddEvent(EventTime eventTime) throws DirtyStateException {
        return tryAddEvent( eventTime.event(), eventTime.time() );
    }

    public boolean tryDeleteEvent(String event, long timeMilli) throws DirtyStateException {
        String key = generateKey( event );
        return retryTemplate.execute( _context -> eventMap.deleteEvent( key, timeMilli ) );
    }

    public boolean tryDeleteEvent(EventTime eventTime) throws DirtyStateException {
        return tryDeleteEvent( eventTime.event(), eventTime.time() );
    }

    public String keyPrefix() {
        return this.keyPrefix;
    }

    public char delimiter() {
        return ELEMENT_DELIMITER;
    }
}
