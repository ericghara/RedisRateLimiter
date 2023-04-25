package com.ericgha.service.snapshot_consumer;

import com.ericgha.dto.EventTime;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Saves snapshots to a map.  Probably only useful for testing.
 */
public class SnapshotSaver implements SnapshotConsumer {

    private final Map<Long, List<EventTime>> snapshots = new HashMap<>();

    /**
     * Ignores snapshots that are an empty list or null;
     * @param version the first input argument
     * @param snapshot the second input argument
     * @throws IllegalStateException if snapshot is not empty and clock already in map
     */
    @Override
    public void accept(Long version, List<EventTime> snapshot) throws IllegalStateException {
        if (snapshots.containsKey( version )) {
            throw new IllegalStateException(String.format("A snapshot with clock %d already exists.", version));
        }
        if (snapshot.isEmpty()) {
            return;
        }
        snapshots.put( version, Collections.unmodifiableList(snapshot) );
    }

    /**
     *
     * @return snapshots by time as an unmodifiable map
     */
    public Map<Long, List<EventTime>> getSnapshots() {
        return Collections.unmodifiableMap(snapshots);
    }
}
