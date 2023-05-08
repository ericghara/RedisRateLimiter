package com.ericgha.dto.message;

import com.ericgha.dto.MessageType;
import com.fasterxml.jackson.annotation.JsonGetter;

/**
 *  An interface for messages.
 */
public interface MessageInterface {

    long clock();

    @JsonGetter
    MessageType messageType();

}
