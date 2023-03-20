package com.ericgha.dao;

import exception.WriteConflictException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

@Repository
public class EventMap {

    private final ValueOperations<String, String> valueOps;
    private final RedisTemplate<String, String> redisTemplate;
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

    void put(String key, String value) {
        valueOps.set( key, value );
    }

    /**
     * @param event
     * @return {@code true} if update occurred {@code false}
     * @throws WriteConflictException if a concurrent modification caused the transaction to abort.
     */
    public boolean putEvent(String event, long timeMilli) throws WriteConflictException {
        TimeConditionalWrite cw = new TimeConditionalWrite( event, timeMilli );
        List<Boolean> found = redisTemplate.execute( cw );
        if (Objects.isNull( found ) || found.size() != 1) {  // size should be 0 on concurrent update (should retry)
            throw new WriteConflictException( "Value changed mid-transaction." );
        }
        return found.get( 0 );
    }

    public String get(String key) {
        return valueOps.get( key );
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
            long curTime = Instant.now().toEpochMilli();
            // present but expired
            if (Long.parseLong( lastTime.toString() ) + eventDurationMillis < curTime) {
                operations.opsForValue().set( key, Long.toString( timestamp ) );
                // returns an empty list if concurrent modification was made
                return operations.exec();
            }
            operations.discard();
            return List.of( false );
        }
    }

}
