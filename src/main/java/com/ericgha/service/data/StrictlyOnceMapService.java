package com.ericgha.service.data;

import com.ericgha.dao.StrictlyOnceMap;
import com.ericgha.domain.KeyMaker;
import com.ericgha.dto.EventHash;
import com.ericgha.dto.EventTime;
import com.ericgha.dto.TimeIsValidDiff;
import com.ericgha.dto.Versioned;
import com.ericgha.service.event_consumer.EventConsumer;
import com.ericgha.service.event_consumer.NoOpEventConsumer;
import org.springframework.lang.NonNull;

import java.time.Instant;
import java.util.Objects;

public class StrictlyOnceMapService implements EventMapService {

    private final KeyMaker keyMaker;

    private final String clockKey;

    private final StrictlyOnceMap eventMap;

    private long eventDurationMillis;
    private EventConsumer invalidator;
    public StrictlyOnceMapService(@NonNull StrictlyOnceMap eventMap, long eventDurationMillis,
                                  @NonNull KeyMaker keyMaker) {
        this.eventMap = eventMap;
        setEventDuration( eventDurationMillis );
        this.keyMaker = keyMaker;
        this.clockKey = keyMaker.generateClockKey();
        this.invalidator = new NoOpEventConsumer();
    }

    public boolean putEvent(EventTime eventTime) {
        String eventKey = keyMaker.generateEventKey( eventTime.event() );
        TimeIsValidDiff diff = eventMap.putEvent( eventKey, eventTime.time(), clockKey, eventDurationMillis );
        if (Objects.isNull( diff.currentVersion() )) {  // currentVersion null when no state change occurred
            return false;
        }
        handleIfInvalidated( diff, eventTime.event() );
        // second check handles far out-of-order events, i.e. eventTime so far in past it does not invalidate
        // the most recent item in EventMapService, but it's not accepted because it predates an item in the map
        return diff.current().isValid() && eventTime.time() == diff.current().time();  // cannot be null
    }

    /**
     *
     * @param eventTime
     * @return
     * @throws IllegalArgumentException if it is impossible to determine the validity because the queried event is
     * in the future or the queried event is in the past by a period {@code > 2*eventDuration}.
     */
    public boolean isValid(EventTime eventTime) throws IllegalArgumentException {
        isValidCheckArguments(eventTime);
        String eventKey = keyMaker.generateEventKey( eventTime.event() );
        EventHash curState = eventMap.getEventHash( eventKey );
        long requestedTime = eventTime.time();
        if (Objects.nonNull( curState.time() ) && curState.time() == requestedTime && curState.isValid()) {
            return true;
        }
        return Objects.nonNull( curState.retired() ) && curState.retired() == requestedTime;

    }

    private void isValidCheckArguments(EventTime eventTime) throws IllegalArgumentException {
        Objects.requireNonNull(eventTime);
        long time = eventTime.time();
        long currentTime = Instant.now().toEpochMilli();
        if (time > currentTime) {
            throw new IllegalArgumentException("Cannot query validity of an event in the future. EventTime: " + eventTime);
        }
        if (time + 2*eventDurationMillis < currentTime) {
            throw new IllegalArgumentException("Cannot query a time more than 2*eventDuration in the past. EventTime: " + eventTime);
        }
    }

    public void setInvalidator(@NonNull EventConsumer invalidHandler) throws IllegalArgumentException {
        Objects.requireNonNull( invalidHandler, "Received a null invalidator" );
        this.invalidator = invalidHandler;
    }

    private void setEventDuration(long millis) throws IllegalArgumentException {
        if (millis < 0) {
            throw new IllegalArgumentException( "Event duration must be a positive long" );
        }
        this.eventDurationMillis = millis;
    }

    public String keyPrefix() {
        return keyMaker.keyPrefix();
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
