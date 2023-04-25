package com.ericgha.dto.message;

import com.ericgha.dto.EventTime;

/**
 * An interface for Event status messages.  Implementations should
 * be designed with serialization in mind.
 */
public interface EventStatusMessageInterface extends MessageInterface {

    EventTime eventTime();

}
