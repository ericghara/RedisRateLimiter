package com.ericgha.service.snapshot_consumer;

import com.ericgha.dto.EventTime;
import com.ericgha.service.EventQueueSnapshotService;

import java.util.List;
import java.util.function.BiConsumer;

/**
 * A consumer for snapshots created by {@link EventQueueSnapshotService}.
 */
public interface SnapshotConsumer extends BiConsumer<Long, List<EventTime>> {

    @Override
    void accept(Long version, List<EventTime> snapshot);
}
