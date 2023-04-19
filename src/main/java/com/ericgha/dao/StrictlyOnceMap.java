package com.ericgha.dao;

import com.ericgha.config.FunctionRedisTemplate;
import com.ericgha.dto.EventHash;
import com.ericgha.dto.EventTime;
import com.ericgha.dto.TimeIsValid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import redis.clients.jedis.Jedis;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

// todo make a note about how things break if event's time is far off, shouldn't be an issue b/c it's server generated

/**
 * A map that tracks events, modelling strictly once behavior for a given period, {@code eventDuration}.
 * <p>
 * {@code event}s are converted into map keys using the format {@code keyPrefix:eventIdentifier:event}.  Each value
 * consists of three fields:
 *
 * <ol>
 *     <li>{@code time}: the most recent timestamp for the event (<em>note:</em> by literal timestamp on the event, not most recent call)</li>
 *     <li>{@code isValid}: if conflicts have occured for the event at time: {@code time}, {@code true} (1) or {@code false} (0)</li>
 *     <li>{@code retired}: the last time that was retired after {@code eventDuration} with no conflicts (i.e. isValid was true throughout {@code EventDuration})</li>
 * </ol>
 *
 * This creates a data structure where it is guaranteed that the validity of an event at a time may be queried for a period
 * of 2*{@code eventDuration} after the event is received.  Beyond that period the data structure may fail to identify that
 * an event at a time was valid.  Therefore, users of this map should guarantee that they query an event's state within
 * one {@code eventDuration} after the expected event completion (i.e. the event's {@code time + eventDuration}).
 * <p>
 * To preserve space in the database events are expired within {@code 2*eventDuration} time after their last modification.
 *
 */
public class StrictlyOnceMap {

    private static final String DELIMITER = ":"; // delimiter between prefix and key

    private final FunctionRedisTemplate<String, Long> stringLongRedisTemplate;
    private final long eventDurationMillis;
    private final String keyPrefix;
    private final String clockKey;
    private final String isValidIdentifier = "is_valid";
    private final String timeIdentifier = "time";
    private final String retiredIdentifier = "retired";
    private final String eventElement = "EVENT";
    private final Logger log;

    public StrictlyOnceMap(long eventDurationMillis,
                           @NonNull String keyPrefix,
                           @NonNull String clockElement,
                           @NonNull FunctionRedisTemplate<String, Long> stringLongRedisTemplate) {
        this.eventDurationMillis = eventDurationMillis;
        this.keyPrefix = keyPrefix;
        this.clockKey = this.encodeKey( clockElement );
        this.stringLongRedisTemplate = stringLongRedisTemplate;
        this.log = LoggerFactory.getLogger( this.getClass().getName() );
    }

    static boolean toBoolean(Long i) throws IllegalArgumentException {
        if (0 == i) {
            return false;
        }
        if (1 == i) {
            return true;
        }
        throw new IllegalArgumentException( "Can only convert 0 or 1 to a boolean." );
    }

    static EventHash toEventHash(List<Long> rawData) throws IllegalArgumentException {
        Objects.requireNonNull(rawData, "Received a null rawData parameter.");
        if (rawData.size() != 3) {
            throw new IllegalStateException("raw Data should be a list of length 3");
        }
        Long time = rawData.get(0);
        Boolean isValid = Objects.isNull(rawData.get(1)) ? null : toBoolean(rawData.get(1) );
        Long retired = rawData.get(2);
        if (Objects.isNull(time) && Objects.isNull(isValid) && Objects.isNull(retired) || Objects.nonNull(time) && Objects.nonNull(isValid)) {
            return new EventHash(time, isValid, retired);
        }
        throw new IllegalArgumentException( "EventHash is in an inconsistent state: " + rawData );
    }

    static TimeIsValid toTimeIsValid(List<Long> rawData) {
        Objects.requireNonNull(rawData, "Received a null rawData parameter.");
        if (rawData.size() != 0 && rawData.size() != 2) {
            throw new IllegalArgumentException( "raw data should have a length of 0 or 2." + rawData );
        }
        if (rawData.size() == 0) {
            return new TimeIsValid(null, null);
        }
        return new TimeIsValid( rawData.get( 0 ), toBoolean( rawData.get( 1 ) ) );
    }

    // for testing
    void setEvent(EventTime eventTime, boolean isValid, @Nullable Long expiryMilli) {
        String eventKey = encodeEvent( eventTime.event() );
        stringLongRedisTemplate.opsForHash()
                .putAll( eventKey, Map.of( timeIdentifier, eventTime.time(), isValidIdentifier, isValid ) );
        if (Objects.nonNull( expiryMilli )) {
            stringLongRedisTemplate.expire( eventKey, expiryMilli, TimeUnit.MILLISECONDS );
        }
    }

    /**
     * Checks if the given {@link EventTime} is valid.  An {@code EventTime} is valid if the event's hash contains the
     * same time as the {@link EventTime} and the {@code isValid} field is {@code 1} (true).  If {@code isValid} is 0
     * (false) or the {@code time} field is not the same as the {@code EventTime} or the {@code time} and
     * {@code isValid} fields are {@code null}.
     *
     * @param eventTime
     * @return true if valid, else false
     * @throws IllegalStateException if state or values of the hash do not match the expected schema.
     */
    public boolean isValid(EventTime eventTime) throws IllegalStateException {
        String prefixedEventTime = encodeEvent( eventTime.event() );
        List<?> values = stringLongRedisTemplate.opsForHash()
                .multiGet( prefixedEventTime, List.of( timeIdentifier, isValidIdentifier ) );
        Long time = (Long) values.get( 0 );
        Long isValid = (Long) values.get( 1 );
        if (Objects.isNull( time ) && Objects.isNull( isValid )) {
            return false;
        }
        if (Objects.isNull( time ) || Objects.isNull( isValid )) {
            throw new IllegalStateException(
                    "isValid and time had inconsistent states. One was null, other and the other non Null." );
        }
        if (isValid == 0L || eventTime.time() != time) {
            return false;
        }
        if (isValid == 1L) {
            return true;
        }
        throw new IllegalStateException( "Expected 0, 1 or null, found: " + isValid );
    }

    private String encodeKey(String... elements) {
        String[] prefixAndElements = new String[elements.length + 1];
        prefixAndElements[0] = keyPrefix;
        System.arraycopy( elements, 0, prefixAndElements, 1, elements.length );
        return String.join( DELIMITER, prefixAndElements );
    }

    // open for testing
    String encodeEvent(@NonNull String event) {
        return encodeKey( eventElement, event );
    }

    /**
     * @param eventTime
     * @return List[oldState, newState], if event is unique (or previously expired) old state will be an empty list.
     * Otherwise, oldState and new state will be [time, isValid].  isValid must be 0 (false) or 1 (true).
     * @throws IllegalStateException if an improperly formatted response is returned from the database
     */
    @SuppressWarnings("unchecked")
    public List<TimeIsValid> putEvent(EventTime eventTime) throws IllegalStateException {
        String eventKey = encodeEvent( eventTime.event() );
        List<List<Long>> rawResult;
        try (Jedis connection = stringLongRedisTemplate.getJedisConnection()) {
            rawResult = (List<List<Long>>) connection.fcall( "put_event", List.of( eventKey, clockKey ),
                                                             List.of( Long.toString( eventTime.time() ),
                                                                      Long.toString( eventDurationMillis ) ) );
        }
        if (Objects.isNull( rawResult )) {
            throw new NullPointerException( "Command put_event must not return null, but received null." );
        }
        if (rawResult.size() != 2) {
            throw new IllegalStateException( "RawResult must have a length of 2: " + rawResult );
        }
        TimeIsValid prevState = toTimeIsValid( rawResult.get(0) );
        TimeIsValid curState = toTimeIsValid( rawResult.get(1) );
        return List.of( prevState, curState );
    }

    //for testing
    EventHash getEventHash(String event) throws IllegalStateException {
        String prefixedEventTime = encodeEvent( event );
        List<Long> values = stringLongRedisTemplate.opsForHash()
                .multiGet( prefixedEventTime, List.of( timeIdentifier, isValidIdentifier, retiredIdentifier ) ).stream()
                .map( l -> (Long) l ).toList();
        return toEventHash(values);
    }

    // for testing
    Long getClock() {
        return stringLongRedisTemplate.opsForValue().get( clockKey );
    }


}
