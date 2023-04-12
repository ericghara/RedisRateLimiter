package com.ericgha.service.snapshot_consumer;

import com.ericgha.dto.EventStatus;
import com.ericgha.dto.message.KeyFrameMessage;
import org.springframework.messaging.simp.SimpMessagingTemplate;

import java.util.List;

public class SnapshotSTOMPMessenger implements SnapshotConsumer<EventStatus> {

    private final SimpMessagingTemplate template;
    private final String prefix;

    public SnapshotSTOMPMessenger(SimpMessagingTemplate template, String prefix) {
        this.template = template;
        this.prefix = prefix;
    }

    @Override
    public void accept(Long timestamp, List<EventStatus> snapshot) {
        KeyFrameMessage keyFrameMessage = new KeyFrameMessage( timestamp, snapshot );
        template.convertAndSend( prefix, keyFrameMessage );
    }

    public String prefix() {
        return this.prefix;
    }

}
