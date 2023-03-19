package com.ericgha.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

@Service
public class EventMap {

    private final ValueOperations<String, String> valueOps;

    private long eventDurationMillis;
    private final RedisTemplate<String, String> redisTemplate;

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

    class ConditionalWrite implements SessionCallback<List<Boolean>> {

        private final String key;
        private final long timestamp;

        ConditionalWrite(String key, long timestamp) {
            this.key = key;
            this.timestamp = timestamp;
        }

        @Override
        @SuppressWarnings( "unchecked" )
        public List<Boolean> execute(RedisOperations operations) throws DataAccessException {
            if (operations.opsForValue().setIfAbsent( key, Long.toString( timestamp ) )) {
                return List.of( true );
            }
            operations.watch( key );
            Object lastTime = operations.opsForValue().get( key );
            operations.multi();
            long curTime = Instant.now().toEpochMilli();
            if (Long.parseLong( lastTime.toString() ) + eventDurationMillis < curTime) {
                operations.opsForValue().set( key, Long.toString( timestamp ) );
                return operations.exec();
            }
            operations.discard();
            return List.of( false );
        }
    }

    void put(String key, String value) {
        valueOps.set( key, value );
    }

    public boolean putEvent(String event) {
        ConditionalWrite cw = new ConditionalWrite( event, Instant.now().toEpochMilli() );
        List<Boolean> found = redisTemplate.execute( cw );
        if (Objects.isNull( found ) || found.size() != 1) {
            return false;
        }
        return found.get( 0 );
    }

    public String get(String key) {
        return valueOps.get( key );
    }

}
