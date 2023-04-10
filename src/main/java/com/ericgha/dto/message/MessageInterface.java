package com.ericgha.dto.message;

import com.ericgha.dto.MessageType;
import com.fasterxml.jackson.annotation.JsonGetter;

public interface MessageInterface {

    long timestamp();

    @JsonGetter
    MessageType messageType();

}
