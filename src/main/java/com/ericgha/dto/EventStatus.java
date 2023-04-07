package com.ericgha.dto;

public record EventStatus(String event, long time, Status status) {

    public EventStatus(EventTime eventTime, Status status) {
        this( eventTime.event(), eventTime.time(), status );
    }

}
