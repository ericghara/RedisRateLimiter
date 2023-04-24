package com.ericgha.service;

import com.ericgha.dto.EventTime;
import com.ericgha.dto.Versioned;
import com.ericgha.service.data.EventQueueService;
import com.ericgha.service.snapshot_consumer.SnapshotConsumer;
import jakarta.annotation.Nullable;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.lang.NonNull;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class EventQueueSnapshotService {

    private final Logger log;
    private final EventQueueService eventQueueService;
    private long periodMilli;
    private boolean isRunning;
    @Nullable
    private SnapshotConsumer snapshotConsumer;
    @Nullable
    private ScheduledExecutorService executorService;
    @Nullable
    private Runnable canceler;

    public EventQueueSnapshotService(@NonNull EventQueueService eventQueueService) {
        this.log = LoggerFactory.getLogger( this.getClass().getName() );
        this.eventQueueService = eventQueueService;
        this.periodMilli = Long.MAX_VALUE;
        this.isRunning = false;
    }

    public synchronized <T> void run(long periodMilli,
                                     @NonNull SnapshotConsumer snapshotConsumer) throws IllegalStateException {
        if (isRunning) {
            throw new IllegalStateException( "Cannot change state to run, this is already running." );
        }
        this.periodMilli = periodMilli;
        this.snapshotConsumer = snapshotConsumer;
        this.executorService = Executors.newSingleThreadScheduledExecutor();
        this.isRunning = true;
        ScheduledFuture<?> futureRunnable = executorService.scheduleAtFixedRate( this::snapshot, 0L, this.periodMilli,
                                                                                 TimeUnit.MILLISECONDS );
        this.canceler = () -> futureRunnable.cancel( false );
    }

    /**
     * @return if the service is running (<em>not</em> if a snapshot is in progress)
     */
    public synchronized boolean isRunning() {
        return this.isRunning;
    }

    /**
     * @return period in milliseconds.  If not running Long.MAX_VALUE.
     */
    public synchronized long periodMilli() {
        return this.periodMilli;
    }

    /**
     * Initiates an orderly shutdown of the this.  Blocks until shutdown is complete.
     *
     * @return true if this was running and therefore a shutdown was performed else false
     */
    @PreDestroy
    public synchronized boolean stop() {
        if (!isRunning) {
            return false;
        }
        this.canceler.run();
        this.periodMilli = Integer.MAX_VALUE;
        this.snapshotConsumer = null;
        this.executorService.close();
        this.executorService = null;
        this.isRunning = false;
        return true;
    }


    @SuppressWarnings({"rawtypes", "unchecked"})
    private void snapshot() {
        try {
            Versioned<List<EventTime>> versionedEvents = eventQueueService.getAll();
            snapshotConsumer.accept( versionedEvents.clock(), versionedEvents.data() );
            log.debug( "Snapshot for queue: {} completed successfully.", eventQueueService.queueKey() );
        } catch (Exception e) {
            log.error( "Snapshot for queue: {} failed with exception: {}", eventQueueService.queueKey(), e );
        }
    }
}
