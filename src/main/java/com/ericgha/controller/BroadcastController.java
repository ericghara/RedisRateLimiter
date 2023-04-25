package com.ericgha.controller;

import com.ericgha.dto.EventTime;
import com.ericgha.service.RateLimiter;
import com.ericgha.service.TimeSyncService;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;

@RestController
public class BroadcastController {

    private final TimeSyncService timeSyncService;

    private final RateLimiter onlyOnceEventService;

    private final RateLimiter strictlyOnceEventService;

    public BroadcastController(TimeSyncService timeSyncService,
                               @Qualifier("onlyOnceEventService") RateLimiter onlyOnceEventService,
                               @Qualifier("strictlyOnceEventService") RateLimiter strictlyOnceService) {
        this.timeSyncService = timeSyncService;
        this.onlyOnceEventService = onlyOnceEventService;
        this.strictlyOnceEventService = strictlyOnceService;
    }

    @MessageMapping("/time")
    @RequestMapping(path = "/time-sync", method = RequestMethod.POST)
    public void beginTimeSync() {
        timeSyncService.beginBroadcast();
    }

    @RequestMapping(path = "/only-once-event", method = RequestMethod.POST)
    public void onlyOnceEvent(@RequestBody String event, HttpServletResponse response) {
        HttpStatus status = onlyOnceEventService.acceptEvent( new EventTime( event, Instant.now().toEpochMilli() ) );
        response.setStatus( status.value() );
    }

    @RequestMapping(path = "/strictly-once-event", method = RequestMethod.POST)
    public void strictlyOnceEvent(@RequestBody String event, HttpServletResponse response) {
        HttpStatus status =
                strictlyOnceEventService.acceptEvent( new EventTime( event, Instant.now().toEpochMilli() ) );
        response.setStatus( status.value() );
    }

}
