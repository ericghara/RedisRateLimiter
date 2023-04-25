package com.ericgha.dto;

/**
 * A DTO for the status of an event.
 * @param event
 * @param time
 * @param status
 * @see Status
 */
public record EventStatus(String event, long time, Status status) {

    public EventStatus(EventTime eventTime, Status status) {
        this( eventTime.event(), eventTime.time(), status );
    }

}
