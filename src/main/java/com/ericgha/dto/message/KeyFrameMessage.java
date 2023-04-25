package com.ericgha.dto.message;

import com.ericgha.dto.EventStatus;
import com.ericgha.dto.MessageType;
import org.springframework.lang.NonNull;

import java.util.List;

/**
 * A DTO for Key Frames.  Key Frames are a snapshot of the Event Queue at a given point in time.  The name and idea
 * is derived from video compression algorithms where a video is composed of key frames and delta frames.
 * @param clock
 * @param snapshot
 */
public record KeyFrameMessage(long clock, @NonNull List<EventStatus> snapshot) implements MessageInterface {

    public static final MessageType MESSAGE_TYPE = MessageType.KEY_FRAME;

    @Override
    public MessageType messageType() {
        return MESSAGE_TYPE;
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof KeyFrameMessage otherMessage) {
            return this.clock == otherMessage.clock && this.snapshot.equals( otherMessage.snapshot);
        }
        return false;
    }

    @Override
    public int hashCode() {
        int result = Long.hashCode( clock );
        result = 31 * result + snapshot.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return String.format( "KeyFrameMessage{clock=%d, snapshot=%s}", clock, snapshot);
    }

}
