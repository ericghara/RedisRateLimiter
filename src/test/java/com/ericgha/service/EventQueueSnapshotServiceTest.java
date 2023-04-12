package com.ericgha.service;

import com.ericgha.dto.EventStatus;
import com.ericgha.dto.EventTime;
import com.ericgha.service.data.EventQueueService;
import com.ericgha.service.event_transformer.EventMapper;
import com.ericgha.service.event_transformer.ToEventStatusAlwaysValid;
import com.ericgha.service.snapshot_consumer.SnapshotSaver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.List;
import java.util.Map;

@ExtendWith(MockitoExtension.class)
public class EventQueueSnapshotServiceTest {

    @Mock
    EventQueueService eventQueueServiceMock;

    EventMapper<EventStatus> eventMapper = new ToEventStatusAlwaysValid();
    SnapshotSaver snapshotsaver;

    EventQueueSnapshotService eventQueueSnapshotService;

    @BeforeEach
    void before() {
        this.eventQueueSnapshotService = new EventQueueSnapshotService( eventQueueServiceMock );
        this.snapshotsaver = new SnapshotSaver();
    }

    @AfterEach
    void after() {
        eventQueueSnapshotService.stop();
    }

    @Test
    @DisplayName("Service creates expected snapshot from eventQueueService")
    void snapshotCreatesExpectedSnapsot() throws InterruptedException {
        List<EventTime> eventTimes = List.of( new EventTime( "one", 1 ), new EventTime( "two", 2 ) );
        Mockito.doReturn( eventTimes ).doReturn( List.of() ).when( eventQueueServiceMock ).getAll();
        eventQueueSnapshotService.run( 10L, eventMapper, snapshotsaver );
        Thread.sleep( 50 );
        Map<Long, List<EventStatus>> snapshots = snapshotsaver.getSnapshots();
        Assertions.assertEquals( 1, snapshots.size(), "only one non-empty snapshot" );
        List<EventStatus> found = snapshots.values().iterator().next();
        List<EventStatus> expected = eventTimes.stream().map( eventMapper ).toList();
        Assertions.assertEquals( expected, found );
    }

    @Test
    @DisplayName("Service takes multiple snapshots over time period")
    void snapshotRunsMultipleTimes() throws InterruptedException {
        List<EventTime> eventTimes = List.of( new EventTime( "one", 1 ), new EventTime( "two", 2 ) );
        Mockito.doReturn( eventTimes ).when( eventQueueServiceMock ).getAll();
        eventQueueSnapshotService.run( 10L, eventMapper, snapshotsaver );
        Thread.sleep( 50 );
        Map<Long, List<EventStatus>> snapshots = snapshotsaver.getSnapshots();
        // expect around 5 or 6 snapshots (first snapshot occurs at T0 then nominally 5 more)
        Assertions.assertTrue( snapshots.size() >= 2, "num snapshots >= 2" );
        Assertions.assertTrue( snapshots.size() < 10, "num snapshots < 10" );
    }

    @Test
    @DisplayName("tryWait blocks while snapshot in progress")
    void tryWaitBlocksDuringSnapshot() {
        EventMapper<EventStatus> delayingMapper = eventTime -> {
            try {
                Thread.sleep( 20 );
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return eventMapper.apply( eventTime );
        };
        // should take ~60 ms to process (3x (sleep 20 ms ) )
        List<EventTime> eventTimes = List.of( new EventTime( "one", 1 ), new EventTime( "two", 2 ),
                                              new EventTime( "three", 3 ) );
        Mockito.doReturn( eventTimes ).when( eventQueueServiceMock ).getAll();
        eventQueueSnapshotService.run( 70L, delayingMapper, snapshotsaver );
        long maxDelay = 0L;
        long stop = Instant.now().toEpochMilli() + 250;
        while (Instant.now().toEpochMilli() < stop) {
            long beforeLock = Instant.now().toEpochMilli();
            eventQueueSnapshotService.tryWait();
            maxDelay = Math.max( Instant.now().toEpochMilli() - beforeLock, maxDelay );
        }
        Assertions.assertTrue( maxDelay >= 30,
                               "Encountered at least 30 ms delay in acquiring lock for 60 ms duration snapshot." );
    }


}
