package com.ericgha.controller;

import com.ericgha.dto.EventTime;
import com.ericgha.dto.message.AddedEventMessage;
import com.ericgha.service.TimeSyncService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;

@RestController
public class BroadcastController {

    private final SimpMessagingTemplate msgTemplate;
    private final String clientPrefix;
    private final TimeSyncService timeSyncService;

    public BroadcastController(TimeSyncService timeSyncService,
                               SimpMessagingTemplate msgTemplate,
                               @Value("${app.web-socket.prefix.client}") String clientPrefix) {
        this.msgTemplate = msgTemplate;
        this.clientPrefix = clientPrefix;
        this.timeSyncService = timeSyncService;
    }

    @MessageMapping("/time")
    @RequestMapping(path = "/time-sync", method = RequestMethod.POST)
    public void beginTimeSync() {
        String prefix = String.format( "%s/%s", clientPrefix, "time" );
        timeSyncService.beginBroadcast( prefix );
    }

    /**
     * Probably want to delete, just a http rest endpoint that triggers broadcast of server time.
     * used for testing
     */
    @RequestMapping(path = "/time", method = RequestMethod.POST)
    public void time() {
        String path = String.format( "%s/%s", clientPrefix, "time" );
        this.msgTemplate.convertAndSend( path, Instant.now().toEpochMilli() );
    }

    @RequestMapping(path = "/event", method = RequestMethod.POST)
    public void event() {
        String path = String.format( "%s/%s", clientPrefix, "event" );
        AddedEventMessage eventMsg = new AddedEventMessage( Instant.now().toEpochMilli(), new EventTime("event", 0) );
        this.msgTemplate.convertAndSend( path, eventMsg );
    }

}