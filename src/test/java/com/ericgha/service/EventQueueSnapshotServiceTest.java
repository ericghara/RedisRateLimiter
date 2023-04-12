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
    void snapshotCreatesExpectedSnapshot() throws InterruptedException {
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
}
