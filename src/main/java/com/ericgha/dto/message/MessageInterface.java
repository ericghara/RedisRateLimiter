package com.ericgha.dto.message;

import com.ericgha.dto.MessageType;

public interface MessageInterface {

    long timestamp();

    MessageType messageType();

}
