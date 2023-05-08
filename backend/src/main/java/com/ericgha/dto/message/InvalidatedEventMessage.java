package com.ericgha.dto.message;

import com.ericgha.dto.EventTime;
import com.ericgha.dto.MessageType;

/**
 * A message dto for an Invalidated Event.
 * @param clock the version clock for the event
 * @param eventTime
 */
public record InvalidatedEventMessage(long clock, EventTime eventTime) implements EventStatusMessageInterface {

    public static final MessageType MESSAGE_TYPE = MessageType.INVALIDATED_EVENT;

    @Override
    public MessageType messageType() {
        return MESSAGE_TYPE;
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof InvalidatedEventMessage otherMessage) {
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
        return String.format( "InvalidatedEventMessage{clock=%d, eventTime=%s}", clock, eventTime);
    }
}
