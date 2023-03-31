package com.ericgha.dao;

import com.ericgha.dto.EventTime;
import exception.DirtyStateException;
import jakarta.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.stereotype.Repository;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

@Repository
public class EventQueue {

    private final RedisTemplate<String, EventTime> eventTimeRedisTemplate;
    private final String queueId;
    private final Logger log = LoggerFactory.getLogger( this.getClass() );

    @Autowired
    public EventQueue(RedisTemplate<String, EventTime> eventTimeRedisTemplate) {
        this( eventTimeRedisTemplate, UUID.randomUUID().toString() );
    }

    public EventQueue(RedisTemplate<String, EventTime> eventTimeRedisTemplate, String queueId) {
        this.eventTimeRedisTemplate = eventTimeRedisTemplate;
        this.queueId = queueId;
    }

    @Nullable
    public EventTime tryPoll(long thresholdTime) throws DirtyStateException {
        TimeConditionalPoll conditionalPoll = new TimeConditionalPoll( queueId, thresholdTime );
        List<EventTime> polled = eventTimeRedisTemplate.execute( conditionalPoll );
        if (polled.size() != 1) {
            if (polled.size() > 1) { // Something is very broken if this occurs
                log.warn( "Too many commands. Results: {}", polled );
                throw new IllegalStateException( "TimeConditionalPoll's transaction executed too many commands" );
            }
            throw new DirtyStateException( String.format( "Queue: %s was modified while polling.", queueId ) );
        }
        return polled.get( 0 );
    }

    /**
     * @param event
     * @return length of queue
     */
    public long offer(EventTime event) {
        // should not return null b/c not used in pipeline or transaction (see documentation)
        return eventTimeRedisTemplate.opsForList().rightPush( queueId, event );
    }

    public long size() {
        // should not return null b/c not used in pipeline or transaction (see documentation)
        return eventTimeRedisTemplate.opsForList().size( queueId );
    }

    public String queueId() {
        return this.queueId;
    }

    // convenience method for testing
    @Nullable
    EventTime peek() {
        return eventTimeRedisTemplate.opsForList().index( queueId, 0 );
    }

    // convenience method for testing
    @Nullable
    EventTime poll() {
        return eventTimeRedisTemplate.opsForList().leftPop( queueId );
    }


    public static class TimeConditionalPoll implements SessionCallback<List<EventTime>> {

        private final String key;
        private final long thresholdTime;
        private final Logger log = LoggerFactory.getLogger( this.getClass() );

        /**
         * @param key           queueId
         * @param thresholdTime latest time that should trigger a poll younger items will not be polled.
         */
        public TimeConditionalPoll(String key, long thresholdTime) {
            this.thresholdTime = thresholdTime;
            this.key = key;
        }

        /**
         * @param operations Redis operations
         * @return <ol>
         * <li>a {@code List} containing a single {@code null} (!) element if the queue was empty or the head of queue does not meet time threshold</li>
         * <li>a single element {@code List} containing the {@link EventTime} polled.</li>
         * <li> an empty {@link List} if queue was modified causing polling to abort</li>
         * </ol>
         * @throws DataAccessException
         */
        @Override
        @SuppressWarnings("unchecked")
        public List<EventTime> execute(RedisOperations operations) throws DataAccessException {
            operations.watch( key );
            EventTime first = (EventTime) operations.opsForList().index( key, 0 );
            if (Objects.isNull( first ) || first.time() > thresholdTime) { // empty queue or need to wait before polling
                operations.unwatch();
                return Arrays.asList( new EventTime[]{null} );
            }
            operations.multi();
            operations.opsForList().leftPop( key );
            return operations.exec();
        }
    }

}
