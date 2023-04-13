package com.ericgha.service.event_consumer;

import com.ericgha.dto.EventTime;
import com.ericgha.dto.Versioned;

import java.util.function.Consumer;

public interface EventConsumer extends Consumer<Versioned<EventTime>> {

    @Override
    void accept(Versioned<EventTime> eventTime);
}
