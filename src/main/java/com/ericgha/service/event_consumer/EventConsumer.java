package com.ericgha.service.event_consumer;

import com.ericgha.dto.EventTime;

import java.util.function.Consumer;

public interface EventConsumer extends Consumer<EventTime> {

    @Override
    void accept(EventTime eventTime);
}
