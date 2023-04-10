package com.ericgha.service;

import com.ericgha.dto.message.MessageInterface;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

@Service
public class ClientUpdaterService {

    private final SimpMessagingTemplate msgTemplate;

    @Autowired
    public ClientUpdaterService(SimpMessagingTemplate msgTemplate) {
        this.msgTemplate = msgTemplate;
    }

    <T extends MessageInterface> void broadcast(String prefix, T message) {
        msgTemplate.convertAndSend( prefix, message );
    }

}
