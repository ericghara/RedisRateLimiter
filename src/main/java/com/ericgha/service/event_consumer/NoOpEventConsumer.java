package com.ericgha.service.event_consumer;

import com.ericgha.dto.EventTime;

public class NoOpEventConsumer implements EventConsumer {
    @Override
    public void accept(EventTime eventTime) {
        // do nothing
    }
}
