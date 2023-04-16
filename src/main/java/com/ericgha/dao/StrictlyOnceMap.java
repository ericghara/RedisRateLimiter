package com.ericgha.dao;

import com.ericgha.dto.EventTime;
import com.ericgha.exception.DirtyStateException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.lang.NonNull;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

// todo make a note about how things break if event's time is far off, shouldn't be an issue b/c it's server generated
public class StrictlyOnceMap {

    private static final String DELIMITER = "."; // delimiter between prefix and key

    private final RedisTemplate<String, Long> stringLongRedisTemplate;
    private final long eventDurationMillis;
    private final String recentEventsPrefix;
    private final String isValidPrefix;
    private final Logger log;

    public StrictlyOnceMap(long eventDurationMillis,
                           @NonNull String recentEventsPrefix,
                           @NonNull String isValidPrefix,
                           @NonNull RedisTemplate<String, Long> stringLongRedisTemplate) {
        this.eventDurationMillis = eventDurationMillis;
        this.recentEventsPrefix = recentEventsPrefix;
        this.isValidPrefix = isValidPrefix;
        this.stringLongRedisTemplate = stringLongRedisTemplate;
        this.log = LoggerFactory.getLogger( this.getClass().getName() );
    }

    public boolean putEvent(EventTime eventTime) throws DirtyStateException {
        ConditionalPut conditionalPut = new ConditionalPut( eventTime );
        Boolean result = stringLongRedisTemplate.execute( conditionalPut );
        if (Objects.isNull( result )) {
            throw new DirtyStateException( "A concurrent modification occurred." );
        }
        return result;
    }

    // for testing
    Long getRecentEvent(String event) {
        String prefixedEvent = encodeEvent( event );
        return stringLongRedisTemplate.opsForValue().get( prefixedEvent );
    }

    // for testing
    void setIsValid(EventTime eventTime, boolean isValid) {
        String prefixedEventTime = encodeEventTime( eventTime.event(), eventTime.time() );
        long validLong = isValid ? 1L : 0L;
        stringLongRedisTemplate.opsForValue()
                .set( prefixedEventTime, validLong, eventDurationMillis * 2, TimeUnit.MILLISECONDS );
    }

    public Boolean isValid(EventTime eventTime) {
        String prefixedEventTime = encodeEventTime( eventTime.event(), eventTime.time() );
        Long value = stringLongRedisTemplate.opsForValue().get( prefixedEventTime );
        // can't switch on a Long...
        if (Objects.isNull( value )) {
            return null;
        }
        if (value == 0L) {
            return false;
        }
        if (value == 1L) {
            return true;
        }
        throw new IllegalStateException( "Expected 0, 1 or null, found: " + value );
    }

    private String stripPrefix(@NonNull String prefixedStr, String prefix) {
        String fullPrefix = isValidPrefix + DELIMITER;
        if (!prefixedStr.startsWith( fullPrefix )) {
            log.warn( "{} does not begin with the prefix: {}", prefixedStr, prefix );
            throw new IllegalArgumentException( "Invalid format.  Input does not begin with the expected prefix." );
        }
        return prefixedStr.substring( fullPrefix.length() );
    }

    private String encodeEventTime(@NonNull String event, long time) {
        return isValidPrefix + DELIMITER + event + '@' + time;  //prefix.event@1234
    }

    private EventTime decodeEventTime(@NonNull String prefixedEventTime) throws IllegalArgumentException {
        String eventTimeStr = stripPrefix( prefixedEventTime, isValidPrefix );
        String[] eventAndTime = eventTimeStr.split( "@(?=\\d+$)", 1 );
        if (eventAndTime.length != 2) {
            throw new IllegalArgumentException( "Invalid format for serialized EventTime: " + eventTimeStr );
        }
        long time;
        try {
            time = Long.parseLong( eventAndTime[1] );
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException( "Invalid format for serialized EventTime: " + eventTimeStr, e );
        }
        return new EventTime( eventAndTime[0], time );
    }

    private String encodeEvent(@NonNull String event) {
        return recentEventsPrefix + DELIMITER + event;
    }

    private String decodeEvent(@NonNull String prefixedEvent) throws IllegalArgumentException {
        return stripPrefix( prefixedEvent, recentEventsPrefix );
    }

    class ConditionalPut implements SessionCallback<Boolean> {

        private final String prefixedEvent;
        private final String prefixedEventTime;
        private final long time;
        private final String event;
        private final long isInvalidTimeout = 2 * eventDurationMillis;


        ConditionalPut(EventTime eventTime) {
            this.time = eventTime.time();
            this.event = eventTime.event();
            this.prefixedEvent = encodeEvent( this.event );
            this.prefixedEventTime = encodeEventTime( this.event, this.time );
        }


        // transaction not required if event not present
        private boolean newEvent(ValueOperations<String, Long> valueOps) {
            if (valueOps.setIfAbsent( prefixedEvent, time, eventDurationMillis, TimeUnit.MILLISECONDS )) {
                // this would cause a concurrent writer to retry , who would subsequently invalidate
                valueOps.set( prefixedEventTime, 1L, isInvalidTimeout, TimeUnit.MILLISECONDS );
                return true;
            }
            return false;
        }

        @SuppressWarnings( "unchecked" )
        public Boolean execute(RedisOperations operations) throws DataAccessException {
            ValueOperations<String, Long> valueOps = operations.opsForValue();
            if (newEvent( valueOps )) {
                return true;
            }
            operations.watch( List.of( prefixedEvent, prefixedEventTime ) );
            Long recentTime = valueOps.get( prefixedEvent );
            recentTime = Objects.isNull( recentTime ) ? 0 : recentTime;  // set far into past if null
            if (recentTime <= time) {
                boolean isValid = recentTime + eventDurationMillis <= time;
                operations.multi();
                valueOps.set( prefixedEvent, time, eventDurationMillis,
                              TimeUnit.MILLISECONDS );  // always need to set recent events to most recent event
                if (isValid) {
                    valueOps.set( prefixedEventTime, 1L, isInvalidTimeout, TimeUnit.MILLISECONDS ); // validate new key
                } else {
                    String recentPrefixedEventTime = encodeEventTime( event, recentTime );
                    valueOps.set( recentPrefixedEventTime, 0L, isInvalidTimeout,
                                  TimeUnit.MILLISECONDS ); // invalidate old  key
                }
                return operations.exec().size() != 0 ? isValid : null;
            }
            // new time *precedes* time in recentEvents map, but no conflict
            else if (time + eventDurationMillis <= recentTime) {
                return false;  // do nothing, reject
            } else {
                // conflict!
                String recentPrefixedEventTime = encodeEventTime( event, recentTime );
                valueOps.set( recentPrefixedEventTime, 0L, isInvalidTimeout, TimeUnit.MILLISECONDS );  // invalidate
                return false; // reject
            }
        }
    }

}
