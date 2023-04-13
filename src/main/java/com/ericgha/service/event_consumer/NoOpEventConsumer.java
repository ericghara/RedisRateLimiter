package com.ericgha.service.event_consumer;

import com.ericgha.dto.EventTime;
import com.ericgha.dto.Versioned;

public class NoOpEventConsumer implements EventConsumer {
    @Override
    public void accept(Versioned<EventTime> VersionedEventTime) {
        // do nothing
    }
}
