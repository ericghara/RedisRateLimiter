package com.ericgha.service.data;

import com.ericgha.dao.EventQueue;
import com.ericgha.dto.EventTime;
import com.ericgha.dto.Versioned;
import com.ericgha.service.event_consumer.EventConsumer;
import jakarta.annotation.Nullable;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;

import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;


/**
 * Polls the {@link EventQueue} and expires events, submitting them to an {@link EventConsumer}.  The amount of time
 * items remain on the queue is determined by the {@code delayMilli}.  The timestamp of the {@link EventTime} is
 * compared to the system time.  When {@code system_time >= delayMilli + event_time} an event <em>may</em> be polled.
 * There are no guarantees when polling will occur, with contention for the queue or the expiry of multiple items in
 * rapid succession creating factors that are outside the control of this service.  However, In times when there is low
 * contention for the queue the {@code pollIntervalMilli} of 10 ms should provide a reasonable approximation for the
 * delay for removing expired items are removed from the queue.
 * <p>
 * <em>Note:</em> the {@code pollIntervalMilli} is only significant during periods of quiescence, during periods with
 * multiple expirations polling occurs as quickly as events may be processed.
 */

// todo 'cleanly' restart dead workers
public class EventExpiryService {

    private final EventQueueService queueService;
    private final Logger log;
    @Nullable
    private WorkerContext workerContext;

    public EventExpiryService(EventQueueService queueService) {
        this.queueService = queueService;
        this.log = LoggerFactory.getLogger( this.getClass() );
        this.workerContext = null;
    }


    /**
     * @param eventConsumer action to be taken upon polling event from queue
     * @param delayMilli    the amount of time events should be <em>aged</em> on the queue.
     * @throws IllegalStateException if {@code EventExpiryService} was already running
     */
    public synchronized void start(EventConsumer eventConsumer, long delayMilli,
                                   int numWorkers) throws IllegalStateException {
        if (Objects.isNull( eventConsumer )) {
            throw new NullPointerException( "Received a null EventConsumer." );
        }
        if (isRunning()) {
            throw new IllegalStateException( "Service already started." );
        }
        workerContext = new WorkerContext( queueService, eventConsumer, numWorkers, delayMilli );
        workerContext.start();
    }

    /**
     * @return previous run state
     */
    @PreDestroy
    public synchronized boolean stop() {
        if (isRunning()) {
            workerContext.stop();
            this.workerContext = null;
            return true;
        }
        return false;
    }

    /**
     * @return current run state
     */
    public boolean isRunning() {
        return Objects.nonNull( workerContext );
    }

    static class WorkerContext {

        private final EventQueueService queueService;
        private final int numWorkers;
        private final Lock activityLock;
        private final Logger log;
        private final int POLL_INTERVAL_MILLI = 10;
        private final EventConsumer doOnExpire;
        private final long delayMilli;
        private volatile boolean shutdownRequested;
        private ExecutorService pool;


        WorkerContext(EventQueueService queueService, EventConsumer doOnExpire, int numWorkers, long delayMilli) {
            this.queueService = queueService;
            this.delayMilli = delayMilli;
            this.doOnExpire = doOnExpire;
            this.numWorkers = numWorkers;
            this.shutdownRequested = true;
            this.log = LoggerFactory.getLogger( this.getClass().getName() );
            this.activityLock = new ReentrantLock();
        }

        synchronized boolean start() {
            if (!shutdownRequested) {
                return false;
            }
            shutdownRequested = false;
            pool = Executors.newFixedThreadPool( numWorkers );
            for (int workerId = 0; workerId < numWorkers; workerId++) {
                submitWorker( workerId );
            }
            return true;
        }

        synchronized boolean stop() {
            if (shutdownRequested) {
                return false;
            }
            shutdownRequested = true;
            pool.close();
            pool = null;
            return true;
        }

        private void submitWorker(int workerId) {
            PollWorker worker = new PollWorker( workerId );
            BiConsumer<Void, Throwable> errorHandler = (_v, e) -> {
                if (Objects.nonNull( e )) {
                    log.error( "Exception for worker {}:", workerId, e );
                }
                log.info( "EventExpiryService:{}:worker-{}: Went down.", queueService.queueKey(), workerId );
            };
            CompletableFuture.runAsync( worker, pool ).whenComplete( errorHandler );
        }

        class PollWorker implements Runnable {
            private final int workerId;
            private final Logger log;

            public PollWorker(int workerId) {
                this.workerId = workerId;
                this.log = LoggerFactory.getLogger( this.getClass().getName() + "-" + this.workerId );
            }

            @Override
            public void run() {
                while (!shutdownRequested) {
                    this.handleActivity();
                }
            }

            public int workerId() {
                return this.workerId;
            }

            private void handleActivity() {
                activityLock.lock();
                Versioned<EventTime> curEvent = null;
                try {
                    // block until activity or shutdown
                    while (!shutdownRequested && Objects.isNull( curEvent )) {
                        curEvent = this.pollQueue();
                        if (Objects.isNull( curEvent )) {
                            try {
                                Thread.sleep( POLL_INTERVAL_MILLI );
                            } catch (InterruptedException e) {
                                log.debug( "Caught an interruptedException", e );
                                Thread.currentThread().interrupt();
                            }
                        }
                    }
                } finally {
                    activityLock.unlock();
                }
                if (Objects.nonNull( curEvent )) { // potentially null on shutdown
                    handleExpire( curEvent );
                }
                this.beginExhaustivePoll(); // detects shutdown itself
            }

            private void handleExpire(Versioned<EventTime> event) {
                try {
                    doOnExpire.accept( event );
                } catch (Exception e) {
                    log.warn( "Encountered an error while expiring event {}.  Status will be lost.", event );
                    log.debug( "Exception on expiring event (version: {}): {}.", event.clock(), e );
                }
            }

            @Nullable
            private Versioned<EventTime> pollQueue() {
                Versioned<EventTime> polledEvent = null;
                long now = Instant.now().toEpochMilli();
                try {
                    polledEvent = queueService.tryPoll( now - delayMilli );
                } catch (DataAccessException e) {
                    return null;  // scale in on failure
                }
                return Objects.nonNull( polledEvent ) ? polledEvent : null;
            }

            void beginExhaustivePoll() {
                while (!shutdownRequested) {
                    Versioned<EventTime> curEvent = this.pollQueue();
                    if (Objects.nonNull( curEvent )) {
                        handleExpire( curEvent );
                    } else {
                        break;  // Queue empty or should wait for events to expire
                    }
                }
            }
        }
    }
}
