package com.ericgha.dao;

import exception.WriteConflictException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Objects;

@Repository
public class EventMap {

    private final ValueOperations<String, String> valueOps;
    private final RedisTemplate<String, String> redisTemplate;
    private final Logger log = LoggerFactory.getLogger( this.getClass() );
    private long eventDurationMillis;

    @Autowired
    EventMap(RedisTemplate<String, String> redisTemplate, @Value("${app.event-duration-millis}") long eventDurationMillis) {
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

    // Convenience method for testing.
    void put(String key, String value) {
        valueOps.set( key, value );
    }

    /**
     * @param event
     * @param timeMilli
     * @return {@code true} if update occurred {@code false}
     * @throws WriteConflictException if a concurrent modification caused the transaction to abort.
     */
    public boolean putEvent(String event, long timeMilli) throws WriteConflictException {
        TimeConditionalWrite condWrite = new TimeConditionalWrite( event, timeMilli );
        List<Boolean> found = redisTemplate.execute( condWrite );
        if (Objects.isNull( found ) || found.size() != 1) {
            log.debug( "Failed to put {} : {}", event, timeMilli );
            throw new WriteConflictException( "Value changed mid-transaction." );
        }
        return found.get(0);
    }

    /**
     *
     * @param event
     * @param timeMilli
     * @return {@code true} if delete performed {@code false} if no delete performed
     * @throws WriteConflictException if a concurrent modification caused the transaction to abort
     */
    public boolean deleteEvent(String event, long timeMilli) throws WriteConflictException {
        TimeConditionalDelete condDelete = new TimeConditionalDelete( event, timeMilli );
        List<String> found = redisTemplate.execute(condDelete);
        if (Objects.isNull( found ) || found.size() != 1) {
            log.debug( "Failed to delete {} : {}", event, timeMilli );
            throw new WriteConflictException( "Value changed mid-transaction." );
        }
        return !found.get(0).isEmpty();
    }

    public String get(String key) {
        return valueOps.get( key );
    }

    static class TimeConditionalDelete implements SessionCallback<List<String>> {

        private final String key;
        private final long timestamp;
        private final Logger log = LoggerFactory.getLogger( this.getClass() );

        public TimeConditionalDelete(String key, long timestamp) {
            this.key = key;
            this.timestamp = timestamp;
        }

        @SuppressWarnings("unchecked")
        public List<String> execute(RedisOperations operations) throws DataAccessException {
            operations.watch( key );
            Object curTimeObj = operations.opsForValue().get( key );
            if (Objects.isNull( curTimeObj )) {
                log.info( "Attempted to delete absent key {}", key );
                return List.of( "" );
            }
            long curTime = Long.parseLong( curTimeObj.toString() );
            operations.multi();
            if (curTime == this.timestamp) {
                operations.opsForValue().getAndDelete( key );
            }
            else if (curTime < this.timestamp) {
                log.warn( "Possible inconsistency: Found a timestamp in map older than key being removed. {} : {}",
                        key, timestamp );
                operations.opsForValue().getAndDelete( key );
            }
            else { // curTime > this.timestamp
                operations.discard();
                return List.of("");
            }
            return operations.exec();
        }
    }

    class TimeConditionalWrite implements SessionCallback<List<Boolean>> {

        private final String key;
        private final long timestamp;

        TimeConditionalWrite(String key, long timestamp) {
            this.key = key;
            this.timestamp = timestamp;
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
            // absent
            if (operations.opsForValue().setIfAbsent( key, Long.toString( timestamp ) )) {
                return List.of( true );
            }
            operations.watch( key );
            Object lastTime = operations.opsForValue().get( key );
            operations.multi();
            // present but expired
            if (Long.parseLong( lastTime.toString() ) + eventDurationMillis < timestamp) {
                operations.opsForValue().set( key, Long.toString( timestamp ) );
                // returns an empty list if concurrent modification was made
                return operations.exec();
            }
            operations.discard();
            return List.of( false );
        }
    }


}
