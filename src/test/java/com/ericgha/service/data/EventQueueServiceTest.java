package com.ericgha.service.data;

import com.ericgha.dao.EventQueue;
import com.ericgha.domain.KeyMaker;
import com.ericgha.dto.EventTime;
import com.ericgha.dto.PollResponse;
import com.ericgha.dto.Versioned;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.Objects;

import static org.mockito.Mockito.mockStatic;

@ExtendWith(MockitoExtension.class)
public class EventQueueServiceTest {

    KeyMaker keyMaker = new KeyMaker( "TEST" );

    MockedStatic<Instant> instantMock;

    @Mock
    EventQueue eventQueue;

    EventQueueService queueService;

    @BeforeEach
    void before() {
        instantMock = mockStatic( Instant.class, Mockito.CALLS_REAL_METHODS );
        queueService = new EventQueueService( eventQueue, keyMaker );
    }

    @AfterEach
    void after() {
        if (Objects.nonNull( instantMock )) {
            instantMock.close();
        }
    }

    @Test
    @DisplayName("approxSize initializes itself on first if not preivously updated")
    void approxSizeGetsTrueSizeOnFirstCall() {
        long expectedSize = 1L;
        Mockito.doReturn( expectedSize ).when( eventQueue ).size( Mockito.anyString() );
        Assertions.assertEquals( expectedSize, queueService.approxSize() );
        Mockito.verify( eventQueue, Mockito.times( 1 ) ).size( Mockito.anyString() );
    }

    @Test
    @DisplayName("approxSize gets true size when lastSize TTL expired")
    void approxSizeGetsTrueSizeWhenTTLExpired() {
        Instant trueNow = Instant.now();
        instantMock.when( Instant::now ).thenReturn( trueNow );
        Mockito.when( eventQueue.size( Mockito.anyString() ) ).thenReturn( 1L );
        Assertions.assertEquals( 1L, queueService.approxSize(), "First call initializes lastSize" );

        Instant nowPlusTTL = trueNow.plusMillis( queueService.LAST_SIZE_TTL_MILLI + 1 );
        instantMock.when( Instant::now ).thenReturn( nowPlusTTL );
        Mockito.when( eventQueue.size( Mockito.anyString() ) ).thenReturn( 2L );
        Assertions.assertEquals( 2L, queueService.approxSize(), "Second call refreshes lastSize" );

    }

    @Test
    @DisplayName("approxSize does not call EventQueue when lastSize TTL has not expired.")
    void approxSizeDoesNotCallEventQueueWhenSizeYoungerThanTTL() {
        long expectedSize = 1L;
        Mockito.doReturn( expectedSize ).when( eventQueue ).size( Mockito.anyString() );
        queueService.size();
        Assertions.assertEquals( 1, queueService.approxSize(), "expected approx size cached froms size query" );
        Mockito.verify( eventQueue,
                        Mockito.times( 1 ).description( "A single call to eventQueue#size should be made." ) )
                .size( Mockito.anyString() );
    }

    @Test
    @DisplayName("offer calls eventQueue with expected arguments")
    void offerCallsEventQueueWithExpectedArgs() {
        Mockito.doReturn( new Versioned<>( 1L, 0L ) ).when( eventQueue )
                .offer( Mockito.any( EventTime.class ), Mockito.anyString(), Mockito.anyString() );
        EventTime eventTime = new EventTime("Test 1", Instant.now().toEpochMilli() );
        queueService.offer(eventTime);
        Mockito.verify( eventQueue, Mockito.times(1) ).offer( eventTime, keyMaker.generateQueueKey(),
                                                              keyMaker.generateClockKey());
    }

    @Test
    @DisplayName( "offer returns expected" )
    void offerReturnsExpected() {
        Versioned<Long> versionedSize = new Versioned<>( 1L, 0L );
        Mockito.doReturn( versionedSize ).when( eventQueue )
                .offer( Mockito.any( EventTime.class ), Mockito.anyString(), Mockito.anyString() );
        EventTime eventTime = new EventTime("Test 1", Instant.now().toEpochMilli() );
        long foundClock = queueService.offer(eventTime);
        Assertions.assertEquals(versionedSize.clock(), foundClock);
    }

    @Test
    @DisplayName("offer updates lastSize when ttl expired")
    void offerUpdatesLastSizeWhenTTLExpired() {
        Versioned<Long> versionedSize = new Versioned<>( 1L, 2L );
        Mockito.doReturn( versionedSize ).when( eventQueue )
                .offer( Mockito.any( EventTime.class ), Mockito.anyString(), Mockito.anyString() );
        EventTime eventTime = new EventTime("Test 1", Instant.now().toEpochMilli() );
        queueService.offer(eventTime);
        Assertions.assertEquals(versionedSize.data(), queueService.approxSize() );
        Mockito.verify(eventQueue, Mockito.never() ).size(Mockito.anyString() );
    }

    @Test
    @DisplayName("tryPoll calls eventQueue#tryPoll with expected threshold time")
    void tryPollCallsEventQueueWithExpectedArgs() {
        long queueSize = 4;
        long thresholdTime = 3;
        PollResponse pollResponse = new PollResponse(queueSize);
        Mockito.doReturn(pollResponse).when(eventQueue).tryPoll( thresholdTime, keyMaker.generateQueueKey(), keyMaker.generateClockKey() );
        queueService.tryPoll(thresholdTime);
    }

    @Test
    @DisplayName("tryPoll returns null when eventQueue returns an empty PollResponse")
    void tryPollReturnsNullWhenNothingToPoll() {
        long queueSize = 4;
        long thresholdTime = 3;
        PollResponse pollResponse = new PollResponse(queueSize);
        Mockito.doReturn(pollResponse).when(eventQueue).tryPoll( Mockito.anyLong(), Mockito.anyString(), Mockito.anyString() );
        Assertions.assertNull(queueService.tryPoll( thresholdTime ));
    }

    @Test
    @DisplayName( "tryPoll return expected Versioned<EventTime> when eventQueue returns a non-empty PollResponse" )
    void tryPollReturnsNonNullWhenEventPolled() {
        // build poll response
        long queueSize = 4;
        long clock = 2;
        Versioned<EventTime> versionedEventTime = new Versioned<>(clock, new EventTime("Test 1", Instant.now().toEpochMilli() ) );
        PollResponse pollResponse = new PollResponse( versionedEventTime, queueSize);
        Mockito.doReturn(pollResponse).when(eventQueue).tryPoll( Mockito.anyLong(), Mockito.anyString(), Mockito.anyString() );
        // test
        long thresholdTime = 3;
        Assertions.assertEquals(versionedEventTime, queueService.tryPoll(thresholdTime));
    }

    @Test
    @DisplayName("tryPoll updates lastSize")
    void tryPollUpdatesLastSize() {
        long queueSize = 4;
        long thresholdTime = 3;
        PollResponse pollResponse = new PollResponse(queueSize);
        Mockito.doReturn(pollResponse).when(eventQueue).tryPoll( Mockito.anyLong(), Mockito.anyString(), Mockito.anyString() );
        queueService.tryPoll( thresholdTime );
        queueService.approxSize();
        Mockito.verify(eventQueue, Mockito.never() ).size(Mockito.anyString() );
    }

}
