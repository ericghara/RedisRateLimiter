package com.ericgha.dto.message;

import com.ericgha.dto.EventStatus;
import com.ericgha.dto.MessageType;
import org.springframework.lang.NonNull;

import java.util.List;

public record KeyFrameMessage(long timestamp, @NonNull List<EventStatus> snapshot) implements MessageInterface {

    public static final MessageType MESSAGE_TYPE = MessageType.KEY_FRAME;

    @Override
    public MessageType messageType() {
        return MESSAGE_TYPE;
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof KeyFrameMessage otherMessage) {
            return this.timestamp == otherMessage.timestamp && this.snapshot.equals(otherMessage.snapshot);
        }
        return false;
    }

    @Override
    public int hashCode() {
        int result = Long.hashCode( timestamp );
        result = 31 * result + snapshot.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return String.format("KeyFrameMessage{timestamp=%d, snapshot=%s}", timestamp, snapshot);
    }

}
