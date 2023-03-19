package com.ericgha.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.jedis.JedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.temporal.TemporalField;
import java.util.Objects;

@Service
public class EventMap {

    private final ValueOperations<String, String> valueOps;

    // consider making constructor parameter, so you can validate;
    @Value("${app.event-duration-millis}")
    private int eventDurationMillis;
    private final RedisTemplate<String, String> redisTemplate;

    @Autowired
    EventMap(RedisTemplate<String,String> redisTemplate) {
        this.valueOps = redisTemplate.opsForValue();
        this.redisTemplate = redisTemplate;
    }

    public void setEventDuration(int millis) {
        this.eventDurationMillis = millis;
    }

    class ConditionalWrite implements SessionCallback<Boolean> {

        private final String key;
        private final long timestamp;

        ConditionalWrite(String key, long timestamp) {
            this.key = key;
            this.timestamp = timestamp;
        }

        @Override
        public Boolean execute(RedisOperations operations) throws DataAccessException {
            if (operations.opsForValue().setIfAbsent(key, Long.toString(timestamp))) {
                return true;
            }
            operations.watch(key);
            operations.multi();
            Object lastTime = operations.opsForValue().get(key);
            long curTime = Instant.now().toEpochMilli();
            if (Long.parseLong(lastTime.toString())+eventDurationMillis < curTime) {
                operations.opsForValue().set( key, Long.toString(timestamp) );
                operations.exec();
                return true;
            }
            return false;
        }
    }

    void test() {
        JedisConnection connection = (JedisConnection) redisTemplate.getConnectionFactory().getConnection();
        connection.multi();
        connection.commands().set(new byte[] {1}, new byte[] {1});
        connection.exec();
    }

    void put(String key, String value) {
        valueOps.set(key, value);
    }

    public boolean putEvent(String event) {
        ConditionalWrite cw = new ConditionalWrite( event, Instant.now().toEpochMilli() );
        return cw.execute( redisTemplate );
    }

    public String get(String key) {
        return valueOps.get(key);
    }

}
