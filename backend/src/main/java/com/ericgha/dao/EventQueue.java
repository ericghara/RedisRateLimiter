package com.ericgha.dao;

import com.ericgha.dto.EventTime;
import com.ericgha.dto.PollResponse;
import com.ericgha.dto.Versioned;
import com.ericgha.service.data.FunctionRedisTemplate;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.lang.NonNull;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import redis.clients.jedis.Jedis;

import java.util.List;
import java.util.Objects;

/**
 * {@code EventQueue} is a FIFO queue similar to a delay queue.  Offering to the queue is unremarkable, however polling
 * is non-standard.  Polling requires a threshold time, elements in the queue with
 * {@code clock  <= current-time - threshold-time} are polled.  An element at the head that doesn't meet this
 * criteria is left on the queue.
 * <p>
 * As this is a FIFO queue, and timestamps are provided by the callers of the queue, out of order events will cause
 * items to blocked.  An item at the tail of the queue could have an older clock than that at the head.  A poll with
 * a threshold time that does not meet the criteria for the head item of the queue will block the tail item (and every other
 * item in the queue meeting the threshold criteria) from being polled.
 * <p>
 * If an implementation requires that the queue remain in temporal order callers to the queue should be orchestrated
 * to ensure that timestamps are monotonically increasing and offered sequentially.
 * <p>
 * Most queue operations return a scalar clock which is a monotonically increasing long.  Calls to {@link EventQueue}
 * may be linearized based upon this clock.
 */
public class EventQueue {

    private final FunctionRedisTemplate<String, String> stringTemplate;
    private final Logger log = LoggerFactory.getLogger( this.getClass() );
    private final ObjectMappingTools objectMappingTools;

    public EventQueue(FunctionRedisTemplate<String, String> stringTemplate, ObjectMapper objectMapper) {
        this.stringTemplate = stringTemplate;
        this.objectMappingTools = new ObjectMappingTools( objectMapper );
    }

    /**
     * @param thresholdTime latest time that should trigger a poll younger items will not be polled, items equal or
     *                      older to threshold will be polled.
     * @return null if nothing polled, else the (versioned) item polled
     * @throws IllegalStateException if an error occurred deserializing the DB response.
     */
    @Nullable
    @Retryable(maxAttemptsExpression = "${app.redis.retry.num-attempts}",
            backoff = @Backoff(delayExpression = "${app.redis.retry.initial-interval}",
                    multiplierExpression = "${app.redis.retry.multiplier}"))
    public PollResponse tryPoll(long thresholdTime, String queueKey, String clockKey) throws IllegalStateException {
        List<?> rawPoll;
        try (Jedis connection = stringTemplate.getJedisConnection()) {
            rawPoll = (List<?>) connection.fcall( "POLL_QUEUE", List.of( queueKey, clockKey ),
                                                  List.of( Long.toString( thresholdTime ) ) );
        } catch (ClassCastException e) {
            throw new IllegalStateException( "Could not deserialize the DB response.", e );
        }
        try {
            return objectMappingTools.toPollResponse( rawPoll );
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException( "Database returned an unexpected response format.", e );
        }
    }

    /**
     * @param event
     * @return Versioned length of queue
     * @throws IllegalArgumentException if any serialization errors occur
     * @throws IllegalStateException    if a non-numeric reply is returned from the DB
     */
    @Retryable(maxAttemptsExpression = "${app.redis.retry.num-attempts}",
            backoff = @Backoff(delayExpression = "${app.redis.retry.initial-interval}",
                    multiplierExpression = "${app.redis.retry.multiplier}"))
    public Versioned<Long> offer(EventTime event, String queueKey,
                                 String clockKey) throws IllegalArgumentException, IllegalStateException {
        String eventJson = objectMappingTools.serializeEventTime( event );
        List<?> rawResult;
        try (Jedis conn = stringTemplate.getJedisConnection()) {
            rawResult = (List<?>) conn.fcall( "OFFER_QUEUE", List.of( queueKey, clockKey ), List.of( eventJson ) );
        }
        if (rawResult.size() != 2) {
            throw new IllegalStateException( "Received an unexpected response form the DB." );
        }
        try {
            return new Versioned<>( (long) rawResult.get( 0 ), (Long) rawResult.get( 1 ) );
        } catch (ClassCastException e) {
            throw new IllegalStateException( "Could not convert the reply to a long." );
        }
    }

    /**
     * Returns a range of elements in the queue.  Indexing semantics follow those of redis lists.  This call is
     * guaranteed to complete atomically.
     * @param start start index
     * @param end end index
     * @return An in order list from start to end index (versioned by scalar clock)
     * @throws IllegalStateException if an error occurs deserializing the database response.
     */
    @Retryable(maxAttemptsExpression = "${app.redis.retry.num-attempts}",
            backoff = @Backoff(delayExpression = "${app.redis.retry.initial-interval}",
                    multiplierExpression = "${app.redis.retry.multiplier}"))
    public Versioned<List<EventTime>> getRange(long start, long end, String queueKey,
                                               String clockKey) throws IllegalStateException {
        List<?> rawResponse;
        try (Jedis conn = stringTemplate.getJedisConnection()) {
            rawResponse = (List<?>) conn.fcall( "RANGE_QUEUE", List.of( queueKey, clockKey ), List.of( "0", "-1" ) );
            return objectMappingTools.getRangeToObj( rawResponse );
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException( "Could not deserialize the DB response.", e );
        }
    }

    /**
     * Get the size of the queue.
     * @param queueKey key for the queue
     * @return long size
     */
    @Retryable(maxAttemptsExpression = "${app.redis.retry.num-attempts}",
            backoff = @Backoff(delayExpression = "${app.redis.retry.initial-interval}",
                    multiplierExpression = "${app.redis.retry.multiplier}"))
    public long size(String queueKey) {
        // should not return null b/c not used in pipeline or transaction (see documentation)
        return stringTemplate.opsForList().size( queueKey );
    }


    @Nullable
        // convenience method for testing
    String peek(String queueKey) {
        return stringTemplate.opsForList().index( queueKey, 0 );
    }

    @Nullable
        // convenience method for testing
    EventTime poll(String queueKey) {
        String rawJson = stringTemplate.opsForList().leftPop( queueKey );
        if (Objects.isNull( rawJson )) {
            return null;
        }
        return objectMappingTools.toEventTime( rawJson );
    }

    @Nullable
    Long getClock(String clockKey) {
        String numStr = stringTemplate.opsForValue().get( clockKey );
        if (Objects.isNull( numStr )) {
            return null;
        }
        return Long.parseLong( numStr );
    }


    // Handling serialization/deserializaiton at class level is a debatable design decision.  Separate RedisTemplates
    // for the one-off return type of tryPoll and one for EventTime felt too niche for such few operations
    static class ObjectMappingTools {

        private final ObjectMapper objectMapper;

        ObjectMappingTools(ObjectMapper objectMapper) {
            this.objectMapper = objectMapper;
        }

        private Versioned<EventTime> toVersionedEventTime(List<?> rawResult) throws IllegalArgumentException {
            long version;
            String jsonData;
            try {
                jsonData = (String) rawResult.get( 0 );
                version = toLong( rawResult, 1 );
            } catch (ClassCastException e) {
                throw new IllegalArgumentException( "Unable to deserialize version to a Long.", e );
            }
            EventTime eventTime = toEventTime( jsonData );
            return new Versioned<>( version, eventTime );
        }

        private long toLong(List<?> rawResult, int index) throws IllegalArgumentException {
            try {
                return (long) rawResult.get( index );
            } catch (Exception e) {
                if (e instanceof ClassCastException || e instanceof NullPointerException ||
                        e instanceof IndexOutOfBoundsException) {
                    throw new IllegalArgumentException( "Improper input format.", e );
                } else {
                    throw e;
                }
            }
        }

        @Nullable
        PollResponse toPollResponse(@NonNull List<?> rawResult) throws IllegalArgumentException {
            return switch (rawResult.size()) {
                case 1 -> new PollResponse( ( toLong( rawResult, 0 ) ) );
                case 3 -> {
                    Versioned<EventTime> versionedEvenTime = toVersionedEventTime( rawResult.subList( 0, 2 ) );
                    long queueSize = toLong( rawResult, 2 );
                    yield new PollResponse( versionedEvenTime, queueSize );
                }
                default -> throw new IllegalArgumentException( "Improper input format." );
            };
        }

        Versioned<List<EventTime>> getRangeToObj(@NonNull List<?> rawResult) throws IllegalArgumentException {
            if (rawResult.size() != 2) {
                throw new IllegalArgumentException( "Improper input format." );
            }
            long version;
            List<EventTime> elements;
            try {
                List<?> rawElements = (List<?>) rawResult.get( 0 );
                version = toLong( rawResult, 1 );
                elements = rawElements.stream().map( rawElement -> toEventTime( (String) rawElement ) ).toList();
            } catch (ClassCastException e) {
                throw new IllegalArgumentException( "Improper input format.  Unexpected types." );
            }
            return new Versioned<>( version, elements );
        }

        String serializeEventTime(@NonNull EventTime eventTime) throws IllegalArgumentException {
            try {
                return objectMapper.writer().writeValueAsString( eventTime );
            } catch (JsonProcessingException e) {
                throw new IllegalArgumentException( "Unable to serialize EventTime.", e );
            }
        }

        EventTime toEventTime(@NonNull String eventTimeJson) {
            try {
                return objectMapper.readValue( eventTimeJson, EventTime.class );
            } catch (JsonProcessingException e) {
                throw new IllegalArgumentException( "Encountered error while deserializing EventTime JSON.", e );
            }
        }
    }


}
