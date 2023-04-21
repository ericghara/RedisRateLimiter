package com.ericgha.controller;

import com.ericgha.dto.EventTime;
import com.ericgha.service.EventService;
import com.ericgha.service.TimeSyncService;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;

@RestController
public class BroadcastController {

    private final SimpMessagingTemplate msgTemplate;
    private final String clientPrefix;
    private final TimeSyncService timeSyncService;

    private final EventService onlyOnceEventService;

    private final EventService strictlyOnceEventService;

    public BroadcastController(TimeSyncService timeSyncService,
                               SimpMessagingTemplate msgTemplate,
                               @Value("${app.web-socket.prefix.client}") String clientPrefix,
                               @Qualifier("onlyOnceEventService") EventService onlyOnceEventService,
                               @Qualifier("strictlyOnceEventService") EventService strictlyOnceService) {
        this.msgTemplate = msgTemplate;
        this.clientPrefix = clientPrefix;
        this.timeSyncService = timeSyncService;
        this.onlyOnceEventService = onlyOnceEventService;
        this.strictlyOnceEventService = strictlyOnceService;
    }

    @MessageMapping("/time")
    @RequestMapping(path = "/time-sync", method = RequestMethod.POST)
    public void beginTimeSync() {
        String prefix = String.format( "%s/%s", clientPrefix, "time" );
        timeSyncService.beginBroadcast( prefix );
    }

    /**
     * Probably want to delete, just a http rest endpoint that triggers broadcast of server time. used for testing
     */
    @RequestMapping(path = "/time", method = RequestMethod.POST)
    public void time() {
        String path = String.format( "%s/%s", clientPrefix, "time" );
        this.msgTemplate.convertAndSend( path, Instant.now().toEpochMilli() );
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
