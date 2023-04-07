package com.ericgha.dto.message;

import com.ericgha.dto.EventTime;
import com.ericgha.dto.MessageType;

public record AddedEventMessage(long timestamp, EventTime eventTime) implements EventStatusMessageInterface {

    public static final MessageType MESSAGE_TYPE = MessageType.ADDED_EVENT;

    @Override
    public MessageType messageType() {
        return MESSAGE_TYPE;
    }


}
