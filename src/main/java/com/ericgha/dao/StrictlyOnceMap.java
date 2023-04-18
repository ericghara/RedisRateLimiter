package com.ericgha.dao;

import com.ericgha.config.FunctionRedisTemplate;
import com.ericgha.dto.EventTime;
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
public class StrictlyOnceMap {

    private static final String DELIMITER = ":"; // delimiter between prefix and key

    private final FunctionRedisTemplate<String, Long> stringLongRedisTemplate;
    private final long eventDurationMillis;
    private final String keyPrefix;
    private final String clockKey;
    private final String isValidIdentifier = "is_valid";
    private final String timeIdentifier = "time";
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

    //    private String stripPrefix(@NonNull String prefixedStr, String prefix) {
//        String fullPrefix = isValidPrefix + DELIMITER;
//        if (!prefixedStr.startsWith( fullPrefix )) {
//            log.warn( "{} does not begin with the prefix: {}", prefixedStr, prefix );
//            throw new IllegalArgumentException( "Invalid format.  Input does not begin with the expected prefix." );
//        }
//        return prefixedStr.substring( fullPrefix.length() );
//    }
//
//    private String encodeEventTime(@NonNull String event, long time) {
//        return isValidPrefix + DELIMITER + event + '@' + time;  //prefix.event@1234
//    }
//
//    private EventTime decodeEventTime(@NonNull String prefixedEventTime) throws IllegalArgumentException {
//        String eventTimeStr = stripPrefix( prefixedEventTime, isValidPrefix );
//        String[] eventAndTime = eventTimeStr.split( "@(?=\\d+$)", 1 );
//        if (eventAndTime.length != 2) {
//            throw new IllegalArgumentException( "Invalid format for serialized EventTime: " + eventTimeStr );
//        }
//        long time;
//        try {
//            time = Long.parseLong( eventAndTime[1] );
//        } catch (NumberFormatException e) {
//            throw new IllegalArgumentException( "Invalid format for serialized EventTime: " + eventTimeStr, e );
//        }
//        return new EventTime( eventAndTime[0], time );
//    }
//
    private String encodeEvent(@NonNull String event) {
        return encodeKey( eventElement, event );
    }
//
//    private String decodeEvent(@NonNull String prefixedEvent) throws IllegalArgumentException {
//        return stripPrefix( prefixedEvent, keyPrefix );
//    }

    /**
     * @param eventTime
     * @return List[oldState, newState], if event is unique (or previously expired) old state will be an empty list.
     * Otherwise, oldState and new state will be [time, isValid].  isValid must be 0 (false) or 1 (true).
     * @throws IllegalStateException
     */
    @SuppressWarnings("unchecked")
    @Nullable
    public List<List<Long>> putEvent(EventTime eventTime) throws IllegalStateException {
        List<List<Long>> rawResult;
        try (Jedis connection = stringLongRedisTemplate.getJedisConnection()) {
            rawResult = (List<List<Long>>) connection.fcall( "put_event", List.of( eventTime.event(), keyPrefix ),
                                                             List.of( Long.toString( eventTime.time() ),
                                                                      Long.toString( eventDurationMillis ) ) );
        }
        if (Objects.isNull( rawResult )) {
            throw new NullPointerException( "Command put_event must not return null, but received null." );
        }
        if (rawResult.size() != 2 || ( rawResult.get( 0 ).size() != 0 && rawResult.get( 0 ).size() != 2 ) ||
                rawResult.get( 1 ).size() != 2) {
            throw new IllegalStateException(
                    "put_event should return List[List[prevTime, prevIsValid], List[curTime, curIsValid]]" );
        }
        return rawResult;
    }

    //for testing
    List<Long> getEventInfo(String event) {
        String prefixedEventTime = encodeEvent( event );
        List<?> values = stringLongRedisTemplate.opsForHash()
                .multiGet( prefixedEventTime, List.of( timeIdentifier, isValidIdentifier ) );
        return values.stream().map( l -> (Long) l ).toList();
    }

    Long getClock() {
        return stringLongRedisTemplate.opsForValue().get(clockKey);
    }

}
