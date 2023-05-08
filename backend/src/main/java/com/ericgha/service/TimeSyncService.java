package com.ericgha.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This service broadcasts server time, allowing clients to synchronize their time to this server.
 */
@Service
public class TimeSyncService {

    public static final long PERIOD_MILLI = 50;
    private final int numFrames;
    private final SimpMessagingTemplate msgTemplate;
    private final String prefix;
    private final AtomicInteger framesRemaining;

    /**
     *
     * @param numFrames the number of timestamps to send
     * @param msgTemplate template to be used for messaging
     */
    public TimeSyncService(@Value("${app.time-sync.num-frames}") int numFrames,
                           @Autowired SimpMessagingTemplate msgTemplate,
                           @Value("${app.time-sync.message-prefix}") String prefix) {
        this.numFrames = numFrames;
        this.msgTemplate = msgTemplate;
        this.prefix = prefix;
        this.framesRemaining = new AtomicInteger( 0 );
    }

    private Runnable broadcaster() {
        return () -> {
            boolean doRun = true;
            while (doRun) {
                int curRemaining = framesRemaining.decrementAndGet();
                doRun = curRemaining > 0;
                try {
                    Thread.sleep( PERIOD_MILLI );
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                msgTemplate.convertAndSend( prefix, Instant.now().toEpochMilli() );
            }
        };
    }

    /**
     * A broadcast consists of at least {@code numFrames} timestamp messages separated with a delay of {@code PERIOD_MILLI}.
     * If there is an ongoing broadcast additional frames are added to it to guarantee {@code numFrames} will be sent
     * to the most recent client.
     */
    public void beginBroadcast() {
        int curRemaining = framesRemaining.getAndSet( numFrames );
        if (curRemaining <= 0) {  // need to start service;
            String threadName = String.format( this.getClass().getName() + "-" + Instant.now().toEpochMilli() % 1000 );
            new Thread( broadcaster(), threadName ).start();
        }
    }
}
