package com.ericgha.service.snapshot_mapper;

import com.ericgha.dto.EventStatus;
import com.ericgha.dto.EventTime;
import com.ericgha.dto.Status;

import java.util.List;

/**
 * Maps a chunk of {@link EventTime}s to {@link EventStatus}s.  All events are mapped to {@code Valid} statuses.
 */
public class ToSnapshotStatusAlwaysValid implements SnapshotMapper<EventStatus> {

    @Override
    public List<EventStatus> apply(List<EventTime> snapshot) {
        return snapshot.stream().map(et -> new EventStatus( et, Status.Valid ) ).toList();
    }

}
