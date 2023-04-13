package com.ericgha.dto.message;

import com.ericgha.dto.EventTime;
import com.ericgha.dto.MessageType;
import org.springframework.lang.NonNull;

public record AddedEventMessage(long timestamp, @NonNull EventTime eventTime) implements EventStatusMessageInterface {

    public static final MessageType MESSAGE_TYPE = MessageType.ADDED_EVENT;

    @Override
    public MessageType messageType() {
        return MESSAGE_TYPE;
    }


    @Override
    public boolean equals(Object other) {
        if (other instanceof AddedEventMessage otherMessage) {
            return this.timestamp == otherMessage.timestamp && this.eventTime.equals( otherMessage.eventTime() );
        }
        return false;
    }

    @Override
    public int hashCode() {
        int result = Long.hashCode( timestamp );
        result = 31 * result + eventTime.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return String.format( "AddedEventMessage{timestamp=%d, eventTime=%s}", timestamp, eventTime );
    }
}
