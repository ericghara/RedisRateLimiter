package com.ericgha.dao;

import com.ericgha.domain.KeyMaker;
import com.ericgha.dto.EventTime;
import com.ericgha.dto.Versioned;
import com.ericgha.exception.DirtyStateException;
import jakarta.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SessionCallback;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class EventQueue {

    private final RedisTemplate<String, EventTime> eventTimeTemplate;
    private final String queueId;
    private final String clockKey;
    private final Logger log = LoggerFactory.getLogger( this.getClass() );

    public EventQueue(RedisTemplate<String, EventTime> eventTimeTemplate, KeyMaker keyMaker) {
        this.eventTimeTemplate = eventTimeTemplate;
        this.queueId = keyMaker.generateQueueKey();
        this.clockKey = keyMaker.generateClockKey();
    }

    /**
     * @param thresholdTime latest time that should trigger a poll younger items will not be polled, items equal or
     *                      older to threshold will be polled.
     * @return null if nothing polled, else the (versioned) item polled
     * @throws DirtyStateException
     */
    @Nullable
    @SuppressWarnings("rawtypes")
    public Versioned<EventTime> tryPoll(long thresholdTime) throws DirtyStateException {
        TimeConditionalPoll conditionalPoll = new TimeConditionalPoll( thresholdTime );
        List polled = eventTimeTemplate.execute( conditionalPoll );
        return switch (polled.size()) {
            case 0 ->
                    throw new DirtyStateException( String.format( "Queue: %s was modified while polling.", queueId ) );
            case 1 -> {
                if (Objects.isNull( polled.get( 0 ) )) {
                    yield null;
                }
                log.warn( "Expected a single null element, found {}", polled );
                throw new IllegalStateException(
                        "Unexpected state.  A single element list should always contain a null element." );
            }
            case 2 -> new Versioned<EventTime>( (long) polled.get( 0 ), (EventTime) polled.get( 1 ) );
            default -> {
                log.warn( "Expected a 0-2 element list: {}", polled );
                throw new IllegalStateException( "Expected 0-2 elements, found more" );
            }
        };
    }

    /**
     * @param event
     * @return version based on an incrementing long
     */
    public long offer(EventTime event) throws DirtyStateException {
        VersionedOffer versionedOffer = new VersionedOffer( event );
        List<Long> clockAndSize = eventTimeTemplate.execute( versionedOffer );
        return switch (clockAndSize.size()) {
            case 0 -> throw new DirtyStateException( "Clock was modified while offering." );
            case 2 -> clockAndSize.get( 0 );
            default -> {
                log.warn( "Expected a 0 or 2 element list, found {}", clockAndSize );
                throw new IllegalStateException(
                        String.format( "Expected a 0 or 2 element list. Found %d elements", clockAndSize.size() ) );
            }
        };
    }

    @SuppressWarnings("unchecked, rawtypes")
    public Versioned<List<EventTime>> getRange(long start, long end) throws DirtyStateException {
        VersionedRange versionedRange = new VersionedRange( start, end );
        List versionAndRange = eventTimeTemplate.execute( versionedRange );
        return switch (versionAndRange.size()) {
            case 0 -> throw new DirtyStateException( "Clock was modified during range query." );
            default -> new Versioned<>( (long) versionAndRange.get( 0 ),
                                        (List<EventTime>) versionAndRange.get( 1 ) );
        };
    }

    public long size() {
        // should not return null b/c not used in pipeline or transaction (see documentation)
        return eventTimeTemplate.opsForList().size( queueId );
    }

    public String clockKey() {
        return this.clockKey;
    }

    public String queueId() {
        return this.queueId;
    }


    @Nullable
        // convenience method for testing
    EventTime peek() {
        return eventTimeTemplate.opsForList().index( queueId, 0 );
    }

    @Nullable
        // convenience method for testing
    EventTime poll() {
        return eventTimeTemplate.opsForList().leftPop( queueId );
    }


    @SuppressWarnings({"rawtypes", "unchecked"})
    public class TimeConditionalPoll implements SessionCallback<List> {

        private final long thresholdTime;
        private final Logger log = LoggerFactory.getLogger( this.getClass() );

        /**
         * @param thresholdTime latest time that should trigger a poll younger items will not be polled.
         */
        public TimeConditionalPoll(long thresholdTime) {
            this.thresholdTime = thresholdTime;
        }

        /**
         * @param operations Redis operations
         * @return <ol>
         * <li>a {@code List} containing a single {@code null} (!) element if the queue was empty or the head of queue does not meet time threshold</li>
         * <li>a two element {@code List} [ (Long) {@code clock}, {@link EventTime} ] polled.</li>
         * <li> an empty {@link List} if queue was modified causing polling to abort</li>
         * </ol>
         * @throws DataAccessException
         */
        @Override
        public List execute(RedisOperations operations) throws DataAccessException {
            operations.watch( List.of( queueId, clockKey ) );
            EventTime first = (EventTime) operations.opsForList().index( queueId, 0 );
            if (Objects.isNull( first ) || first.time() > thresholdTime) { // empty queue or need to wait before polling
                operations.unwatch();
                return Arrays.asList( new EventTime[]{null} );
            }
            operations.multi();
            operations.opsForValue().increment( clockKey );
            operations.opsForList().leftPop( queueId );
            return operations.exec();
        }
    }

    public class VersionedOffer implements SessionCallback<List<Long>> {

        private final EventTime eventTime;

        VersionedOffer(EventTime eventTime) {
            this.eventTime = eventTime;
        }

        /**
         * @param operations Redis operations
         * @return Empty {@link List} if {@code clockKey} was concurrently modified else returns
         * {@code [version, queue size]}
         * @throws DataAccessException
         */
        @Override
        @SuppressWarnings("unchecked")
        public List<Long> execute(RedisOperations operations) throws DataAccessException {
            operations.watch( clockKey );
            operations.multi();
            operations.opsForValue().increment( clockKey );
            operations.opsForList().rightPush( queueId, eventTime );
            return operations.exec();
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public class VersionedRange implements SessionCallback<List> {

        private final long start;
        private final long end;

        VersionedRange(long start, long end) {
            this.start = start;
            this.end = end;
        }

        /**
         * @param operations Redis operations
         * @return empty list if {@code clockKey} was concurrently modified, else a list
         * {@code [ (long) clock, [EventTimes...] ]} <em>note: </em> range data is a nested list
         * @throws DataAccessException
         */
        @Override
        public List execute(RedisOperations operations) throws DataAccessException {
            operations.watch( clockKey );
            operations.multi();
            operations.opsForValue().increment( clockKey );
            eventTimeTemplate.opsForList().range( queueId, start, end );
            return operations.exec();
        }

    }


}
