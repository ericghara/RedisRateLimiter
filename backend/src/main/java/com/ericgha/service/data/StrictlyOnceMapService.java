package com.ericgha.service.data;

import com.ericgha.dao.StrictlyOnceMap;
import com.ericgha.domain.KeyMaker;
import com.ericgha.dto.EventHash;
import com.ericgha.dto.EventTime;
import com.ericgha.dto.Status;
import com.ericgha.dto.TimeIsValidDiff;
import com.ericgha.dto.Versioned;
import com.ericgha.service.event_consumer.EventConsumer;
import com.ericgha.service.event_consumer.NoOpEventConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.lang.NonNull;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Abstracts lower level details of {@link StrictlyOnceMap} performing argument validation and translation of raw
 * objects returned from the DB into domain objects.  Additionally, offers a {@code invalidator} callback to handle
 * invalidation of events.
 */
public class StrictlyOnceMapService implements EventMapService {

    private final KeyMaker keyMaker;

    private final String clockKey;

    private final StrictlyOnceMap eventMap;

    private long eventDurationMillis;
    private EventConsumer invalidator;
    private final Logger log;

    /**
     *
     * @param eventMap the DAO this service should use.  Multiple {@code StrictlyOnceMapService}s may share the same DAO.
     * @param eventDurationMillis how long an event should incubate before being published (if not invalidated in the meantime).
     * @param keyMaker generates keys used by this.
     */
    public StrictlyOnceMapService(@NonNull StrictlyOnceMap eventMap, long eventDurationMillis,
                                  @NonNull KeyMaker keyMaker) {
        this.log = LoggerFactory.getLogger( this.getClass().getName() + ":" + keyMaker.keyPrefix() );
        this.eventMap = eventMap;
        setEventDuration( eventDurationMillis );
        this.keyMaker = keyMaker;
        this.clockKey = keyMaker.generateClockKey();
        this.invalidator = new NoOpEventConsumer();
    }

    /**
     * Puts an event into the map.  If the put triggers the invalidation of a previously valid event the {@code invalidator}
     * callback is invoked.
     *
     * @param eventTime
     * @return true if the event {@code isValid} after the put, else false.
     */
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
     * Queries the validity of an {@link EventTime}.
     * @param eventTime
     * @return {@code Valid}, {@code Invalid}, {@code Unknown} when the queried event is in the future or greater than
     * {@code 2*eventDurationMilli} in the past.
     */
    public Status isValid(EventTime eventTime) {
        if (!isValidCheckArguments( eventTime )) {
            log.warn( "Received an out of range query." );
            log.debug( "Status query for {} was out of range.", eventTime );
            return Status.Unknown;
        }
        String eventKey = keyMaker.generateEventKey( eventTime.event() );
        EventHash curState = eventMap.getEventHash( eventKey );
        long wantedTime = eventTime.time();
        // check current/most recent event
        return determineStatus( curState, wantedTime );
    }

    /**
     * Checks validity of multiple {@code EventTime}s.
     * @param eventTimes events to query
     * @return an in order list of the event statuses
     * @see StrictlyOnceMap
     */
    public List<Status> isValid(List<EventTime> eventTimes) {
        List<String> eventKeys = eventTimes.stream().map( EventTime::event ).map( keyMaker::generateEventKey ).toList();
        List<EventHash> eventHashes = eventMap.multiGetEventHash( eventKeys );
        List<Status> statuses = new ArrayList<>( eventTimes.size() );

        for (int i = 0; i < eventHashes.size(); i++) {
            EventTime eventTime = eventTimes.get( i );
            // we could filter these out from multiGetEventHash but since this should be rare and the penalty for a
            // pipelined request is low, decided simplify logic and filter later
            if (!isValidCheckArguments( eventTime )) {
                log.warn( "Received an out of range query." );
                log.debug( "Status query for {} was out of range.", eventTime );
                statuses.add( Status.Unknown );
            } else {
                Status status = determineStatus( eventHashes.get( i ), eventTime.time() );
                statuses.add( status );
            }
        }
        return statuses;
    }

    public void setInvalidator(@NonNull EventConsumer invalidHandler) throws IllegalArgumentException {
        Objects.requireNonNull( invalidHandler, "Received a null invalidator" );
        this.invalidator = invalidHandler;
    }

    /**
     *
     * @return the root of the keyspace of this
     */
    public String keyPrefix() {
        return keyMaker.keyPrefix();
    }

    /**
     *
     * @return the clock key being used
     */
    public String clockKey() {
        return clockKey;
    }

    /**
     * @return milliseconds
     */
    public long eventDuration() {
        return eventDurationMillis;
    }

    private Status determineStatus(EventHash eventHash, long wantedTime) {
        if (Objects.nonNull( eventHash.time() ) && eventHash.time() == wantedTime && eventHash.isValid()) {
            return Status.Valid;
        }
        // check retired event
        if (Objects.nonNull( eventHash.retired() ) && eventHash.retired() == wantedTime) {
            return Status.Valid;
        }
        return Status.Invalid;
    }

    private boolean isValidCheckArguments(EventTime eventTime) {
        Objects.requireNonNull( eventTime );
        long time = eventTime.time();
        long currentTime = Instant.now().toEpochMilli();
        if (time > currentTime) {
            log.warn( "Cannot query validity of an event in the future. EventTime: {}.", eventTime );
            return false;
        }
        if (time + 2 * eventDurationMillis < currentTime) {
            log.warn( "Cannot query a time more than 2*eventDuration in the past. EventTime: {}.", eventTime );
            return false;
        }
        return true;
    }

    private void setEventDuration(long millis) throws IllegalArgumentException {
        if (millis < 0) {
            throw new IllegalArgumentException( "Event duration must be a positive long" );
        }
        this.eventDurationMillis = millis;
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
