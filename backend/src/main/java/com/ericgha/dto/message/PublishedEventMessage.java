package com.ericgha.dto.message;

import com.ericgha.dto.EventTime;
import com.ericgha.dto.MessageType;
import org.springframework.lang.NonNull;

/**
 * A DTO for a published event.
 * @param clock scalar version clock
 * @param eventTime
 */
public record PublishedEventMessage(long clock, @NonNull EventTime eventTime) implements EventStatusMessageInterface {

    public static final MessageType MESSAGE_TYPE = MessageType.PUBLISHED_EVENT;

    @Override
    public MessageType messageType() {
        return MESSAGE_TYPE;
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof PublishedEventMessage otherMessage) {
            return this.clock == otherMessage.clock && this.eventTime.equals( otherMessage.eventTime());
        }
        return false;
    }

    @Override
    public int hashCode() {
        int result = Long.hashCode( clock );
        result = 31 * result + eventTime.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return String.format( "PublishedEventMessage{clock=%d, eventTime=%s}", clock, eventTime);
    }
}
