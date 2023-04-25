package com.ericgha.dao;

import com.ericgha.dto.EventHash;
import com.ericgha.dto.TimeIsValid;
import com.ericgha.dto.TimeIsValidDiff;
import com.ericgha.service.data.FunctionRedisTemplate;
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
 * A map that tracks events, modelling strictly once behavior for a given period, {@code expiryMilli}.
 * <p>
 * An {@code eventKey} should uniquely identify a type of event.  For each {@code EventKey} a data structure is created
 * containing:
 *
 * <ol>
 *     <li>{@code time}: the most recent clock for the event (<em>note:</em> by literal clock on the event, not most recent call)</li>
 *     <li>{@code isValid}: if conflicts have occurred for the event at time: {@code time}, {@code true} (1) or {@code false} (0)</li>
 *     <li>{@code retired}: the last time that was retired after {@code eventDuration} with no conflicts (i.e. isValid was true throughout {@code EventDuration})</li>
 * </ol>
 * <p>
 * It is guaranteed that the validity of an event at a time may be queried for a period
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

    /**
     * Puts an event into the map.  If there is a conflicting event it is invalidated.  If another an event with the
     * same key exists but {@code eventDurationMillis } has passed without conflict, the time for that event is retired
     * (i.e. stored under the retired key of the hash). All put keys have an expiry of {@code 2*eventDuration}.
     *
     * @param eventKey unique key identifying the event type
     * @param time time of event
     * @param clockKey key to the scalar integer for parallelization
     * @param eventDurationMillis the duration of the event, put of an identical key within {@code eventDration} would
     *                            cause invalidation.
     * @return {@link TimeIsValidDiff} which is a representation of the previous state for the key, the current
     * state after the put and the scalar clock
     * @throws IllegalStateException if a malformed response is returned from the database
     */
    @Retryable(maxAttemptsExpression = "${app.redis.retry.num-attempts}",
            backoff = @Backoff(delayExpression = "${app.redis.retry.initial-interval}",
                    multiplierExpression = "${app.redis.retry.multiplier}"))
    public TimeIsValidDiff putEvent(@NonNull String eventKey, long time, @NonNull String clockKey,
                                    long eventDurationMillis) throws IllegalStateException {
        List<?> rawResult;
        try (Jedis connection = stringLongTemplate.getJedisConnection()) {
            rawResult = (List<?>) connection.fcall( "put_event", List.of( eventKey, clockKey ),
                                                    List.of( Long.toString( time ),
                                                             Long.toString( eventDurationMillis ) ) );
        }
        if (Objects.isNull( rawResult )) {
            throw new IllegalStateException( "Command put_event returned null, but it should never return null." );
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
            throw new IllegalStateException( "Received an improperly formatted database response.", e );
        }
    }

    /**
     * A representation of the current and past state of the key (within {@code 2 * eventDurationMilli}.
     *
     * @param eventKey unique identifier of a type of event
     * @return EventHash
     * @throws IllegalStateException
     */
    @Retryable(maxAttemptsExpression = "${app.redis.retry.num-attempts}",
            backoff = @Backoff(delayExpression = "${app.redis.retry.initial-interval}",
                    multiplierExpression = "${app.redis.retry.multiplier}"))
    public EventHash getEventHash(@NonNull String eventKey) throws IllegalStateException {
        List<Long> values = stringLongTemplate.opsForHash()
                .multiGet( eventKey, List.of( timeIdentifier, isValidIdentifier, retiredIdentifier ) ).stream()
                .map( l -> (Long) l ).toList();
        try {
            return toEventHash( values );
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException("Received a malformed or improper response from the database.", e);
        }
    }

    /**
     * A pipelined version of {@link StrictlyOnceMap#getEventHash}
     *
     * @param eventKeys keys to query
     * @return a list of event hashes, in the same order as the eventKeys query
     * @throws IllegalStateException if an unexpected response format is returned from the database.
     */
    @Retryable(maxAttemptsExpression = "${app.redis.retry.num-attempts}",
            backoff = @Backoff(delayExpression = "${app.redis.retry.initial-interval}",
                    multiplierExpression = "${app.redis.retry.multiplier}"))
    @SuppressWarnings("unchecked")
    public List<EventHash> multiGetEventHash(@NonNull List<String> eventKeys) throws IllegalStateException {
        List<?> rawHashes = stringLongTemplate.executePipelined( new SessionCallback<List<List<Long>>>() {

            @Override
            public List<List<Long>> execute(@NonNull RedisOperations operations) throws DataAccessException {
                HashOperations<String, String, Long> opsForHash = operations.opsForHash();
                for (String eventKey : eventKeys) {
                    opsForHash.multiGet( eventKey, List.of( timeIdentifier, isValidIdentifier, retiredIdentifier ) );
                }
                return null;
            }
        } );
        try {
            return rawHashes.stream().map( l -> (List<Long>) l ).map( StrictlyOnceMap::toEventHash ).toList();
        } catch (ClassCastException e) {
            throw new IllegalStateException( "Received an unexpected response format from the database.", e );
        }
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
        // first case is an unknown key, second case is a known key
        if (Objects.isNull( time ) && Objects.isNull( isValid ) && Objects.isNull( retired ) ||
                Objects.nonNull( time ) && Objects.nonNull( isValid )) {
            return new EventHash( time, isValid, retired );
        }
        // something is wrong with the map...
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

}
