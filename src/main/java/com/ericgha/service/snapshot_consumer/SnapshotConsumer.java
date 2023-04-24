package com.ericgha.service.snapshot_consumer;

import com.ericgha.dto.EventTime;

import java.util.List;
import java.util.function.BiConsumer;

public interface SnapshotConsumer extends BiConsumer<Long, List<EventTime>> {

    @Override
    void accept(Long version, List<EventTime> snapshot);
}
