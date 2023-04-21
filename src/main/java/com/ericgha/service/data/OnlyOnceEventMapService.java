package com.ericgha.service.data;

import com.ericgha.dao.OnlyOnceMap;
import com.ericgha.domain.KeyMaker;
import com.ericgha.dto.EventTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

/**
 * Event map DAO is not prefix aware.  Injects prefix to multiplex multiple EventMaps on one DB instance.
 * <p>
 * <em>Note: </em> Decided to take multiplex approach over using multiple DBs as this is a much lighter
 * and does not require multiple connection factories.
 */
public class OnlyOnceEventMapService implements EventMapService {

    private final Logger log;
    private final OnlyOnceMap eventMap;
    private final KeyMaker keyMaker;
    private final long eventDurationMilli;

    public OnlyOnceEventMapService(OnlyOnceMap eventMap, long eventDurationMilli, KeyMaker keyMaker) {
        this.log = LoggerFactory.getLogger( this.getClass().getName() + ":" + keyMaker.keyPrefix() );
        this.eventDurationMilli = validateEventDuration( eventDurationMilli );
        this.eventMap = eventMap;
        this.keyMaker = keyMaker;
    }

    public boolean putEvent(String event, long timeMilli) {
        String key = keyMaker.generateEventKey( event );
        long expireTimeMilli = timeMilli + eventDurationMilli;
        long now = Instant.now().toEpochMilli();
        if (expireTimeMilli <= now) {
            log.warn( "Received an event which has already ended." );
            return false;
        }
        if (timeMilli > now) {
            log.warn( "Received an event beginning in the future." );
            return false;
        }
        return eventMap.putEvent( key, timeMilli, expireTimeMilli );
    }

    public boolean putEvent(EventTime eventTime) {
        return putEvent( eventTime.event(), eventTime.time() );
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
