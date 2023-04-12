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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

public class EventQueueSnapshotService {

    private final EventQueueService eventQueueService;
    private long periodMilli;
    private final ReentrantLock lock;
    private final AtomicBoolean inProgress;
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
        this.inProgress = new AtomicBoolean( false );
        this.lock = new ReentrantLock();
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

    /**
     * Waits if a snapshot is in-progress.  Not intended to immediately prevent concurrent modifications of queue but to
     * allow existing modifications to complete and block new ones from starting.
     * <p>
     * Since snapshot is a relatively long operation, and the queue has high contention an <em>essentially</em>
     * pessimistic locking strategy was desired during the snapshot, but optimistic locking for other times. Retrying
     * and optimistic locking of queue should catch suffice to allow modifications initiated before the lock complete.
     */
    public void tryWait() {
        if (this.inProgress.get()) {
            this.lock.lock(); // wait for snapshot or waiting threads to release lock
            this.lock.unlock();
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private void snapshot() {
        this.lock.lock();
        if (this.inProgress.getAndSet( true )) {
            throw new IllegalStateException( "A snapshot was already in progress.  This should not happen." );
        }
        List<EventTime> events = eventQueueService.getAll();
        List<?> snapshot = events.stream().map( eventMapper ).toList();
        long timestamp = Instant.now().toEpochMilli();
        this.inProgress.set( false );
        this.lock.unlock();
        // this#run enforces same type for EventMapper<T> and SnapshotConsumer<T>
        snapshotConsumer.accept( timestamp, (List) snapshot );
    }


}
