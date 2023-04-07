package com.ericgha.dto.message;

import com.ericgha.dto.EventStatus;
import com.ericgha.dto.MessageType;

import java.util.List;

public record KeyFrameMessage(long timestamp, List<EventStatus> snapshot) implements MessageInterface {

    public static final MessageType MESSAGE_TYPE = MessageType.KEY_FRAME;

    @Override
    public MessageType messageType() {
        return MESSAGE_TYPE;
    }

}
