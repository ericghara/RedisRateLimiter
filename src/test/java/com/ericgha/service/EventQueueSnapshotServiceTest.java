package com.ericgha.service;

import com.ericgha.dto.EventStatus;
import com.ericgha.dto.EventTime;
import com.ericgha.dto.Versioned;
import com.ericgha.service.data.EventQueueService;
import com.ericgha.service.event_transformer.EventMapper;
import com.ericgha.service.event_transformer.ToEventStatusAlwaysValid;
import com.ericgha.service.snapshot_consumer.SnapshotSaver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

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
        Versioned<List<EventTime>> versionedEvents =
                new Versioned<>( 1L, List.of( new EventTime( "one", 1 ), new EventTime( "two", 2 ) ) );
        Versioned<List<EventTime>> versionedEmpty = new Versioned<>( 2L, List.of() );
        Mockito.doReturn( versionedEvents ).doReturn( versionedEmpty ).when( eventQueueServiceMock ).getAll();
        eventQueueSnapshotService.run( 10L, eventMapper, snapshotsaver );
        Thread.sleep( 20 );
        Map<Long, List<EventStatus>> snapshots = snapshotsaver.getSnapshots();
        Assertions.assertEquals( 1, snapshots.size(), "only one non-empty snapshot" );
        Assertions.assertTrue( snapshots.containsKey( 1L ), "Snapshot of expected time exists" );
        List<EventStatus> found = snapshots.get( 1L );
        List<EventStatus> expected = versionedEvents.data().stream().map( eventMapper ).toList();
        Assertions.assertEquals( expected, found, "Snapshot contains expected data" );
    }

    @Test
    @DisplayName("Service takes multiple snapshots over time period")
    void snapshotRunsMultipleTimes() throws InterruptedException {
        List<EventTime> eventTimes = List.of( new EventTime( "one", 1 ), new EventTime( "two", 2 ) );
        AtomicLong clock = new AtomicLong( 0L ); // snapshot saver throws on duplicate timestamps of non-empty snapshots
        Mockito.doAnswer( a -> new Versioned<>( clock.incrementAndGet(), eventTimes ) )
                .when( eventQueueServiceMock ).getAll();
        eventQueueSnapshotService.run( 10L, eventMapper, snapshotsaver );
        Thread.sleep( 50 );
        Map<Long, List<EventStatus>> snapshots = snapshotsaver.getSnapshots();
        // expect around 5 or 6 snapshots (first snapshot occurs at T0 then nominally 5 more)
        Assertions.assertTrue( snapshots.size() >= 2, "num snapshots >= 2" );
        Assertions.assertTrue( snapshots.size() < 10, "num snapshots < 10" );
    }

    @Test
    @Timeout(value = 200, unit = TimeUnit.MILLISECONDS)
    @DisplayName("Shutdown takes < 200 ms with no load")
    void stopShutsDownService() {
        Versioned<List<EventTime>> versionedEmpty = new Versioned<>( 1L, List.of() );
        Mockito.lenient().doReturn( versionedEmpty ).when( eventQueueServiceMock ).getAll();
        eventQueueSnapshotService.run( 10L, eventMapper, snapshotsaver );
        eventQueueSnapshotService.stop();
        // empirically this test takes about 10 ms, timeout is very generous
    }

    @Test
    @DisplayName("isRunning returns false when not running")
    void isRunningReturnsFalse() {
        Assertions.assertFalse( eventQueueSnapshotService.isRunning() );
    }

    @Test
    @DisplayName("isRunning returns true when is running")
    void isRunningReturnsTrue() {
        Versioned<List<EventTime>> versionedEmpty = new Versioned<>( 1L, List.of() );
        Mockito.lenient().doReturn( versionedEmpty ).when( eventQueueServiceMock ).getAll();
        eventQueueSnapshotService.run( 10L, eventMapper, snapshotsaver );
        Assertions.assertTrue( eventQueueSnapshotService.isRunning() );
    }

    @Test
    @DisplayName("periodMilli returns Long.MAX_VALUE when !this.isRunning()")
    void periodMilliReturnsMAX_VALUE() {
        Assertions.assertEquals( Long.MAX_VALUE, eventQueueSnapshotService.periodMilli() );
    }

    @Test
    @DisplayName("periodMilli returns expected period when this.isRunning()")
    void periodMilliReturnsExpectedPeriod() {
        final long periodMilli = 10L;
        Versioned<List<EventTime>> versionedEmpty = new Versioned<>( 1L, List.of() );
        Mockito.lenient().doReturn( versionedEmpty ).when( eventQueueServiceMock ).getAll();
        eventQueueSnapshotService.run( periodMilli, eventMapper, snapshotsaver );
        Assertions.assertEquals( periodMilli, eventQueueSnapshotService.periodMilli() );
    }

    @Test
    @DisplayName("Run throws IllegalStateException when already running")
    void runThrowsWhenAlreadyRunning() {
        Versioned<List<EventTime>> versionedEmpty = new Versioned<>( 1L, List.of() );
        Mockito.lenient().doReturn( versionedEmpty ).when( eventQueueServiceMock ).getAll();
        eventQueueSnapshotService.run( 10L, eventMapper, snapshotsaver );
        Assertions.assertThrows( IllegalStateException.class,
                                 () -> eventQueueSnapshotService.run( 10L, eventMapper, snapshotsaver ) );
    }

}
