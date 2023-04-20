package com.ericgha.service.data;

import com.ericgha.dao.StrictlyOnceMap;
import com.ericgha.dto.EventHash;
import com.ericgha.dto.EventTime;
import com.ericgha.dto.TimeIsValidDiff;
import com.ericgha.dto.Versioned;
import com.ericgha.service.event_consumer.EventConsumer;
import com.ericgha.service.event_consumer.NoOpEventConsumer;
import org.springframework.lang.NonNull;

import java.util.Objects;

public class StrictlyOnceMapService {

    public final String EVENT_ELEMENT = "EVENT";
    public final String CLOCK_ELEMENT = "CLOCK";
    public final String DELIMITER = ":";
    private final String keyPrefix;

    private final String clockKey;

    private final StrictlyOnceMap eventMap;

    private long eventDurationMillis;
    private EventConsumer invalidator;
    public StrictlyOnceMapService(@NonNull StrictlyOnceMap eventMap, long eventDurationMillis,
                                  @NonNull String keyPrefix) {
        this.eventMap = eventMap;
        setEventDuration( eventDurationMillis );
        this.keyPrefix = keyPrefix;
        this.clockKey = encodeKey( CLOCK_ELEMENT );
        this.invalidator = new NoOpEventConsumer();
    }

    public boolean putEvent(EventTime eventTime) {
        String eventKey = encodeEvent( eventTime.event() );
        TimeIsValidDiff diff = eventMap.putEvent( eventKey, eventTime.time(), clockKey, eventDurationMillis );
        if (Objects.isNull( diff.currentVersion() )) {  // currentVersion null when no state change occurred
            return false;
        }
        handleIfInvalidated( diff, eventTime.event() );
        // second check handles far out-of-order events, i.e. eventTime so far in past it does not invalidate
        // the most recent item in EventMap, but it's not accepted because it predates an item in the map
        return diff.current().isValid() && eventTime.time() == diff.current().time();  // cannot be null
    }

    public boolean isValid(EventTime eventTime) {
        String eventKey = encodeEvent( eventTime.event() );
        EventHash curState = eventMap.getEventHash( eventKey );
        long requestedTime = eventTime.time();
        if (Objects.nonNull( curState.time() ) && curState.time() == requestedTime && curState.isValid()) {
            return true;
        }
        return Objects.nonNull( curState.retired() ) && curState.retired() == requestedTime;

    }

    public void setInvalidator(@NonNull EventConsumer invalidHandler) throws IllegalArgumentException {
        Objects.requireNonNull( invalidHandler, "Received a null invalidator" );
        this.invalidator = invalidHandler;
    }

    public void setEventDuration(long millis) throws IllegalArgumentException {
        if (millis < 0) {
            throw new IllegalArgumentException( "Event duration must be a positive long" );
        }
        this.eventDurationMillis = millis;
    }

    public String keyPrefix() {
        return keyPrefix;
    }

    public String clockKey() {
        return clockKey;
    }

    /**
     *
     * @return milliseconds
     */
    public long eventDuration() {
        return eventDurationMillis;
    }

    private String encodeKey(String... elements) {
        String[] prefixAndElements = new String[elements.length + 1];
        prefixAndElements[0] = keyPrefix;
        System.arraycopy( elements, 0, prefixAndElements, 1, elements.length );
        return String.join( DELIMITER, prefixAndElements );
    }
    // open for testing

    String encodeEvent(@NonNull String event) {
        return encodeKey( EVENT_ELEMENT, event );
    }

    private boolean handleIfInvalidated(TimeIsValidDiff diff, String event) {
        Boolean wasValid = diff.previous().isValid();
        // no fields in current can be null and no fields in previous can be null if wasValid is nonNull
        if (Objects.nonNull( wasValid ) && wasValid && !diff.current().isValid()) { // transition from valid to invalid
            Versioned<EventTime> toInvalidate =
                    new Versioned<>( diff.currentVersion(), new EventTime( event, diff.previous().time() ) );
            invalidator.accept( toInvalidate );
            return true;
        }
        return false;
    }
}
