package com.ericgha.dao;

import com.ericgha.service.data.FunctionRedisTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.SetParams;

import java.util.Objects;

/**
 * A map that only adds keys if they do not exist and sets a TTL for all added keys.
 */
public class OnlyOnceMap {

    private final ValueOperations<String, String> valueOps;
    private final FunctionRedisTemplate<String, String> redisTemplate;
    private final Logger log = LoggerFactory.getLogger( this.getClass() );

    public OnlyOnceMap(FunctionRedisTemplate<String, String> redisTemplate) {
        this.valueOps = redisTemplate.opsForValue();
        this.redisTemplate = redisTemplate;
    }


    /**
     * Attempts to put event (@code{eventKey}) into map.  Put succeeds if no identical eventKey is in the map.  Puts set
     * an expiry of {@code eventDuration}.  Since redis guarantees 0-1ms of expire accuracy, if a key is in the map
     * it can be considered non-expired
     *
     * <pre>IF newEvent is absent OR (oldTime + eventDuration) <= newTime then PUT in map</pre>
     *
     * @param eventKey key for the event, it's recommended to prefix keys to prevent collisions with other items in
     *                 the database.
     * @param timeMilli     time of eventKey
     * @param expireAtMilli when event should expire
     * @return {@code true} if update occurred {@code false}
     */
    @Retryable(maxAttemptsExpression = "${app.redis.retry.num-attempts}", backoff = @Backoff(delayExpression = "${app.redis.retry.initial-interval}", multiplierExpression = "${app.redis.retry.multiplier}"))
    public boolean putEvent(String eventKey, long timeMilli, long expireAtMilli) {
        String reply;
        // redis template doesn't allow NX with PX option for set
        try (Jedis conn = redisTemplate.getJedisConnection()) {
            reply = conn.set( eventKey, Long.toString( timeMilli ), SetParams.setParams().nx().pxAt( expireAtMilli ) );
        }
        return Objects.nonNull( reply );
    }

    Long get(String eventKey) {
        String longStr = valueOps.get( eventKey );
        if (Objects.isNull( longStr )) {
            return null;
        }
        return Long.parseLong( longStr );
    }

    // Convenience method for testing.  Returns previous key value or null
    String put(String key, String value) {
        return valueOps.getAndSet( key, value );
    }

}
