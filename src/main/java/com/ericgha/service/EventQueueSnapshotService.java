package com.ericgha.service;

import com.ericgha.dto.EventTime;
import com.ericgha.service.data.EventQueueService;
import com.ericgha.service.event_transformer.EventMapper;
import com.ericgha.service.snapshot_consumer.SnapshotConsumer;
import jakarta.annotation.Nullable;
import org.springframework.lang.NonNull;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class EventQueueSnapshotService {

    private final EventQueueService eventQueueService;
    private long periodMilli;
    private boolean isRunning;
    @Nullable
    private EventMapper<?> eventMapper;
    @Nullable
    private SnapshotConsumer<?> snapshotConsumer;
    @Nullable
    private ScheduledExecutorService executorService;
    @Nullable
    private Runnable canceler;

    public EventQueueSnapshotService(@NonNull EventQueueService eventQueueService) {
        this.eventQueueService = eventQueueService;
        this.periodMilli = Long.MAX_VALUE;
        this.isRunning = false;
    }

    public synchronized <T> void run(long periodMilli, @NonNull EventMapper<T> eventMapper,
                                     @NonNull SnapshotConsumer<T> snapshotConsumer) throws IllegalStateException {
        if (isRunning) {
            throw new IllegalStateException( "Cannot change state to run, this is already running." );
        }
        this.periodMilli = periodMilli;
        this.eventMapper = eventMapper;
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
    public synchronized boolean stop() {
        if (!isRunning) {
            return false;
        }
        this.canceler.run();
        this.periodMilli = Integer.MAX_VALUE;
        this.eventMapper = null;
        this.snapshotConsumer = null;
        this.executorService.close();
        this.executorService = null;
        this.isRunning = false;
        return true;
    }


    @SuppressWarnings({"rawtypes", "unchecked"})
    private void snapshot() {
        List<EventTime> events = eventQueueService.getAll().data();
        long timestamp = Instant.now().toEpochMilli();
        List<?> snapshot = events.stream().map( eventMapper ).toList();
        // this#run enforces same type for EventMapper<T> and SnapshotConsumer<T>
        snapshotConsumer.accept( timestamp, (List) snapshot );
    }


}
