package com.ericgha.service.snapshot_mapper;

import com.ericgha.dto.EventTime;

import java.util.List;
import java.util.function.Function;


public interface SnapshotMapper<T> extends Function<List<EventTime>, List<T>> {
}
