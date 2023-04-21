package com.ericgha.service.status_mapper;

import com.ericgha.dto.EventTime;

import java.util.function.Function;


public interface EventMapper<T> extends Function<EventTime, T> {
}
