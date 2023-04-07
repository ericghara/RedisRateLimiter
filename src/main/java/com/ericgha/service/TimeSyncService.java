package com.ericgha.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class TimeSyncService {

    private final int numFrames;
    private final SimpMessagingTemplate msgTemplate;
    private final AtomicInteger framesRemaining;

    public TimeSyncService(@Value("${app.time-sync.num-frames}") int numFrames,
                           @Autowired SimpMessagingTemplate msgTemplate) {
        this.numFrames = numFrames;
        this.msgTemplate = msgTemplate;
        this.framesRemaining = new AtomicInteger( 0 );
    }

    private Runnable broadcaster(String prefix) {
        return () -> {
            boolean doRun = true;
            while (doRun) {
                int curRemaining = framesRemaining.decrementAndGet();
                doRun = curRemaining > 0;
                try {
                    Thread.sleep( 50 );
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                msgTemplate.convertAndSend( prefix, Instant.now().toEpochMilli() );
            }
        };
    }

    /**
     * @param prefix channel broadcast should be made to
     */
    public void beginBroadcast(String prefix) {
        int curRemaining = framesRemaining.getAndSet( numFrames );
        if (curRemaining <= 0) {  // need to start service;
            String threadName = String.format( this.getClass().getName() + "-" + Instant.now().toEpochMilli() % 1000 );
            new Thread( broadcaster( prefix ), threadName ).start();
        }
    }
}
