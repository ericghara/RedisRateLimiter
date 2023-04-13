package com.ericgha.service.event_transformer;

import com.ericgha.dto.EventStatus;
import com.ericgha.dto.EventTime;
import com.ericgha.dto.Status;

public class ToEventStatusAlwaysValid implements EventMapper<EventStatus> {

    @Override
    public EventStatus apply(EventTime eventTime) {
        return new EventStatus( eventTime, Status.Valid );
    }

}
