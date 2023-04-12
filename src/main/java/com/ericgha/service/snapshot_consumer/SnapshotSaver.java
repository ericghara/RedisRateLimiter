package com.ericgha.service.snapshot_consumer;

import com.ericgha.dto.EventStatus;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SnapshotSaver implements SnapshotConsumer<EventStatus> {

    private final Map<Long, List<EventStatus>> snapshots = new HashMap<>();

    /**
     * Ignores snapshots that are an empty list or null;
     * @param timestamp the first input argument
     * @param snapshot the second input argument
     * @throws IllegalStateException
     */
    @Override
    public void accept(Long timestamp, List<EventStatus> snapshot) throws IllegalStateException {
        if (snapshots.containsKey( timestamp )) {
            throw new IllegalStateException(String.format("A snapshot with timestamp %d already exists.", timestamp));
        }
        if (snapshot.isEmpty()) {
            return;
        }
        snapshots.put( timestamp, Collections.unmodifiableList(snapshot) );
    }

    /**
     *
     * @return snapshots by time as an unmodifiable map
     */
    public Map<Long, List<EventStatus>> getSnapshots() {
        return Collections.unmodifiableMap(snapshots);
    }
}
