package com.ericgha.dao;

import com.ericgha.config.FunctionRedisTemplate;
import com.ericgha.dto.EventHash;
import com.ericgha.dto.TimeIsValid;
import com.ericgha.dto.TimeIsValidDiff;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
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
 * <p>
 * This creates a data structure where it is guaranteed that the validity of an event at a time may be queried for a period
 * of 2*{@code eventDuration} after the event is received.  Beyond that period the data structure may fail to identify that
 * an event at a time was valid.  Therefore, users of this map should guarantee that they query an event's state within
 * one {@code eventDuration} after the expected event completion (i.e. the event's {@code time + eventDuration}).
 * <p>
 * To preserve space in the database events are expired within {@code 2*eventDuration} time after their last modification.
 */
public class StrictlyOnceMap {

    private final FunctionRedisTemplate<String, Long> stringLongTemplate;

    private final String isValidIdentifier = "is_valid";
    private final String timeIdentifier = "time";
    private final String retiredIdentifier = "retired";
    private final Logger log;

    public StrictlyOnceMap(@NonNull FunctionRedisTemplate<String, Long> stringLongTemplate) {
        this.stringLongTemplate = stringLongTemplate;
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
        Objects.requireNonNull( rawData, "Received a null rawData parameter." );
        if (rawData.size() != 3) {
            throw new IllegalStateException( "raw Data should be a list of length 3" );
        }
        Long time = rawData.get( 0 );
        Boolean isValid = Objects.isNull( rawData.get( 1 ) ) ? null : toBoolean( rawData.get( 1 ) );
        Long retired = rawData.get( 2 );
        if (Objects.isNull( time ) && Objects.isNull( isValid ) && Objects.isNull( retired ) ||
                Objects.nonNull( time ) && Objects.nonNull( isValid )) {
            return new EventHash( time, isValid, retired );
        }
        throw new IllegalArgumentException( "EventHash is in an inconsistent state: " + rawData );
    }

    static TimeIsValid toTimeIsValid(List<?> rawData) throws ClassCastException {
        Objects.requireNonNull( rawData, "Received a null rawData parameter." );
        if (rawData.size() != 0 && rawData.size() != 2) {
            throw new IllegalArgumentException( "raw data should have a length of 0 or 2." + rawData );
        }
        if (rawData.size() == 0) {
            return new TimeIsValid( null, null );
        }
        return new TimeIsValid( (long) rawData.get( 0 ), toBoolean( (long) rawData.get( 1 ) ) );
    }

    // for testing, expiry optional.  No expiration set if null.
    void setEvent(String eventKey, long time, boolean isValid, @Nullable Long expiryMilli) {
        stringLongTemplate.opsForHash()
                .putAll( eventKey, Map.of( timeIdentifier, time, isValidIdentifier, isValid ? 1L : 0L ) );
        if (Objects.nonNull( expiryMilli )) {
            stringLongTemplate.expire( eventKey, expiryMilli, TimeUnit.MILLISECONDS );
        }
    }

    @Retryable(maxAttemptsExpression = "${app.redis.retry.num-attempts}", backoff = @Backoff(delayExpression = "${app.redis.retry.initial-interval}", multiplierExpression = "${app.redis.retry.multiplier}"))
    public TimeIsValidDiff putEvent(@NonNull String eventKey, long time, @NonNull String clockKey,
                                    long eventDurationMillis) throws IllegalStateException {
        List<?> rawResult;
        try (Jedis connection = stringLongTemplate.getJedisConnection()) {
            rawResult = (List<?>) connection.fcall( "put_event", List.of( eventKey, clockKey ),
                                                    List.of( Long.toString( time ),
                                                             Long.toString( eventDurationMillis ) ) );
        }
        if (Objects.isNull( rawResult )) {
            throw new NullPointerException( "Command put_event must not return null, but received null." );
        }
        if (rawResult.size() != 2 && rawResult.size() != 3) {
            throw new IllegalStateException( "RawResult must have a length of 2: " + rawResult );
        }
        try {
            TimeIsValid prevState = toTimeIsValid( (List<?>) rawResult.get( 0 ) );
            TimeIsValid curState = toTimeIsValid( (List<?>) rawResult.get( 1 ) );
            Long version = rawResult.size() == 3 ? (long) rawResult.get( 2 ) : null;
            return new TimeIsValidDiff( prevState, curState, version );
        } catch (ClassCastException e) {
            throw new IllegalArgumentException( "Received an improperly formatted database response.", e );
        }
    }

    @Retryable(maxAttemptsExpression = "${app.redis.retry.num-attempts}", backoff = @Backoff(delayExpression = "${app.redis.retry.initial-interval}", multiplierExpression = "${app.redis.retry.multiplier}"))
    public EventHash getEventHash(@NonNull String eventKey) throws IllegalStateException {
        List<Long> values = stringLongTemplate.opsForHash()
                .multiGet( eventKey, List.of( timeIdentifier, isValidIdentifier, retiredIdentifier ) ).stream()
                .map( l -> (Long) l ).toList();
        return toEventHash( values );
    }

    @Retryable(maxAttemptsExpression = "${app.redis.retry.num-attempts}", backoff = @Backoff(delayExpression = "${app.redis.retry.initial-interval}", multiplierExpression = "${app.redis.retry.multiplier}"))
    @SuppressWarnings("unchecked")
    public List<EventHash> multiGetEventHash(@NonNull List<String> eventKeys) throws IllegalStateException {
        List<?> rawHashes = (List<?>) stringLongTemplate.executePipelined( new SessionCallback<List<List<Long>>>() {

            @Override
            public List<List<Long>> execute(@NonNull RedisOperations operations) throws DataAccessException {
                HashOperations<String,String,Long> opsForHash = operations.opsForHash();
                for (String eventKey : eventKeys) {
                   opsForHash.multiGet( eventKey, List.of( timeIdentifier, isValidIdentifier, retiredIdentifier ) );
                }
                return null;
            }
        });
        try {
            return rawHashes.stream().map(l -> (List<Long>) l).map(StrictlyOnceMap::toEventHash).toList();
        } catch (ClassCastException e) {
            throw new IllegalStateException("Received an unexpected response format from the database.", e);
        }
    }

    // for testing
    Long getClock(String clockKey) {
        return stringLongTemplate.opsForValue().get( clockKey );
    }


}
