package com.ericgha.service.event_transformer;

import com.ericgha.dto.EventTime;

import java.util.function.Function;


public interface EventMapper<T> extends Function<EventTime, T> {
}
