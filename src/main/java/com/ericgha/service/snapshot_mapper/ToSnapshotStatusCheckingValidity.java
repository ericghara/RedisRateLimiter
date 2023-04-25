package com.ericgha.service.snapshot_mapper;

import com.ericgha.dto.EventStatus;
import com.ericgha.dto.EventTime;
import com.ericgha.dto.Status;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Maps a chunk of {@link EventTime}s to {@link EventStatus}s using a provided {@code validator} function to determine
 * the statuses in each chunk.
 */
public class ToSnapshotStatusCheckingValidity implements SnapshotMapper<EventStatus> {

    private final Function<List<EventTime>, List<Status>> validator;

    /**
     *
     * @param validator used to map the {@code EventTime}s in a chunk to {@code Status}s
     */
    public ToSnapshotStatusCheckingValidity(Function<List<EventTime>, List<Status>> validator) {
        this.validator = validator;
    }

    @Override
    public List<EventStatus> apply(List<EventTime> eventTimes) {
        List<Status> statusResults = validator.apply( eventTimes );
        List<EventStatus> eventStatuses = new ArrayList<>( eventTimes.size() );
        for (int i = 0; i < eventTimes.size(); i++) {
            EventTime curEvent = eventTimes.get( i );
            Status curStatus = statusResults.get(i);
            eventStatuses.add( new EventStatus( curEvent, curStatus ) );
        }
        return eventStatuses;
    }
}
