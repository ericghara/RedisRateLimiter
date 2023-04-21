package com.ericgha.service.status_mapper;

import com.ericgha.dto.EventStatus;
import com.ericgha.dto.EventTime;
import com.ericgha.dto.Status;

import java.util.function.Predicate;

public class ToEventStatusCheckingValidity implements EventMapper<EventStatus> {

    private final Predicate<EventTime> validator;

    public ToEventStatusCheckingValidity(Predicate<EventTime> validator) {
        this.validator = validator;
    }

    @Override
    public EventStatus apply(EventTime eventTime) {
        if (validator.test(eventTime)) {
            return new EventStatus( eventTime, Status.Valid);
        }
        return new EventStatus(eventTime, Status.Invalid);
    }
}
