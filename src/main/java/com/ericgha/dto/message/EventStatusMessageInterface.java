package com.ericgha.dto.message;

import com.ericgha.dto.EventTime;

public interface EventStatusMessageInterface extends MessageInterface {

    EventTime eventTime();

}
