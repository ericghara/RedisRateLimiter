package com.ericgha.dao;

import com.ericgha.dto.EventTime;
import com.ericgha.exception.DirtyStateException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

public class EventMap {

    private final ValueOperations<String, String> valueOps;
    private final StringRedisTemplate redisTemplate;
    private final Logger log = LoggerFactory.getLogger( this.getClass() );
    private long eventDurationMillis;

    // todo add prefix to keys
    public EventMap(StringRedisTemplate redisTemplate,
                    long eventDurationMillis) {
        this.valueOps = redisTemplate.opsForValue();
        this.redisTemplate = redisTemplate;
        this.setEventDuration( eventDurationMillis );
    }

    public void setEventDuration(long millis) throws IllegalArgumentException {
        if (millis < 0) {
            throw new IllegalArgumentException( "Event duration must be a positive long" );
        }
        this.eventDurationMillis = millis;
    }

    // Convenience method for testing.  Returns previous key value or null
    String put(String key, String value) {
        return valueOps.getAndSet( key, value );
    }

    /**
     * Attempts to put event into map.  Put succeeds if no identical event is in the map, or an identical event is in
     * the map with at a time of at least {@code eventDurationMillis} before this event's {@code timeMilli}
     *
     * <pre>IF newEvent is absent OR (oldTime + eventDuration) <= newTime then PUT in map</pre>
     *
     * @param event
     * @param timeMilli time of event
     * @return {@code true} if update occurred {@code false}
     * @throws DirtyStateException if a concurrent modification caused the transaction to abort.
     * @see TimeConditionalWrite#TimeConditionalWrite(String, long)
     */
    public boolean putEvent(String event, long timeMilli) throws DirtyStateException {
        TimeConditionalWrite condWrite = new TimeConditionalWrite( event, timeMilli );
        List<Boolean> found = redisTemplate.execute( condWrite );
        if (Objects.isNull( found ) || found.size() != 1) {
            log.debug( "Failed to put {} : {}", event, timeMilli );
            throw new DirtyStateException( "Value changed mid-transaction." );
        }
        return found.get( 0 );
    }

    /**
     * Deletes an event from the map if it is equal to or older than {@code timeMilli}. {@code timeMilli}
     * <em>must</em> be at least {@code eventDurationMillis} in the past.
     *
     * @param event
     * @param timeMilli time of event
     * @return {@code true} if delete performed {@code false} if no delete performed
     * @throws DirtyStateException      if a concurrent modification caused the transaction to abort
     * @throws IllegalArgumentException if {@code timeMilli} is not at least {@code eventDurationMillis} in the past
     */
    public boolean deleteEvent(String event, long timeMilli) throws DirtyStateException, IllegalArgumentException {
        if (Instant.now().toEpochMilli() < timeMilli + eventDurationMillis) {
            throw new IllegalArgumentException(
                    String.format( "It is too soon to attempt to delete Event: %s at %d", event, timeMilli ) );
        }
        TimeConditionalDelete condDelete = new TimeConditionalDelete( event, timeMilli );
        List<String> found = redisTemplate.execute( condDelete );
        if (Objects.isNull( found ) || found.size() != 1) {
            log.debug( "Failed to delete {} : {}", event, timeMilli );
            throw new DirtyStateException( "Value changed mid-transaction." );
        }
        return !found.get( 0 ).isEmpty();
    }

    public String get(String key) {
        return valueOps.get( key );
    }

    static class TimeConditionalDelete implements SessionCallback<List<String>> {

        private final String event;
        private final long thresholdTime;
        private final Logger log = LoggerFactory.getLogger( this.getClass() );

        /**
         * A more recent event than threshold <em>will not</em> be removed.  Events at or before threshold <em>will</em>
         * be removed.
         * <p>
         * Most Commonly, the threshold should be equal to the event time in the map.  An event in map older than
         * threshold will be removed, but is a sign inconsistency in the form of a lost event (event that was expected
         * to be added but never was), and will trigger a warning.
         *
         * @param Event         in most practical cases will be an {@code Event}
         * @param thresholdTime reference time, in most cases will be the {@code time} of an {@link EventTime} that we
         *                      are trying to delete.
         */
        public TimeConditionalDelete(String Event, long thresholdTime) {
            this.event = Event;
            this.thresholdTime = thresholdTime;
        }

        /**
         * @param operations Redis operations
         * @return An empty List if a concurrent modification occurred, a single element list with an empty string if no
         * delete was performed, a single element list only containing the deleted value if a deletion occurred.
         * @throws DataAccessException
         */
        @SuppressWarnings("unchecked")
        public List<String> execute(RedisOperations operations) throws DataAccessException {
            operations.watch( event );
            Object foundTimeObj = operations.opsForValue().get( event );
            if (Objects.isNull( foundTimeObj )) {
                operations.unwatch();
                log.info( "Attempted to delete absent key {}", event );
                return List.of( "" );
            }
            long foundTime = Long.parseLong( foundTimeObj.toString() );
            operations.multi();
            if (foundTime == this.thresholdTime) {
                operations.opsForValue().getAndDelete( event );
            } else if (foundTime < this.thresholdTime) {
                log.warn( "Possible inconsistency: Found a timestamp in map older than key being removed. {} : {}",
                          event, thresholdTime );
                operations.opsForValue().getAndDelete( event );
            } else { // foundTime > this.timestamp
                operations.discard();
                return List.of( "" );
            }
            return operations.exec();
        }
    }

    class TimeConditionalWrite implements SessionCallback<List<Boolean>> {

        private final String event;
        private final long time;

        /**
         * If no identical event exists in the map the new event will be added.  If an identical {@code event} exists in
         * the map, its {@code time} will be compared to the new event's {@code time}. If insufficient time
         * ({@code eventDuration}) has passed between the old and new events, the old event will <em>not</em> be
         * overwritten.  Otherwise, the old event's {@code time} will be overwritten with the new event's time.
         *
         * <pre>IF newEvent is absent OR (oldTime + eventDuration) <= newTime then PUT in map</pre>
         *
         * @param event event to be written
         * @param time  time of the event
         */
        TimeConditionalWrite(String event, long time) {
            this.event = event;
            this.time = time;
        }

        /**
         * @param operations Redis operations
         * @return A single element list of {@code true} if write occurred, {@code false} if write did not occur or an
         * empty list if a concurrent modification caused the transaction to abort.
         * @throws DataAccessException
         */
        @Override
        @SuppressWarnings("unchecked")
        public List<Boolean> execute(RedisOperations operations) throws DataAccessException {
            // absent can modify atomically
            if (operations.opsForValue().setIfAbsent( event, Long.toString( time ) )) {
                return List.of( true );
            }
            // optimistic lock
            operations.watch( event );
            Object lastTime = operations.opsForValue().get( event );
            operations.multi();
            // present but expired
            if (Long.parseLong( lastTime.toString() ) + eventDurationMillis <= time) {
                operations.opsForValue().set( event, Long.toString( time ) );
                // returns an empty list if concurrent modification was made
                return operations.exec();
            }
            operations.discard();
            return List.of( false );
        }
    }


}
