package com.ericgha.dto;

import java.io.Serial;
import java.io.Serializable;
import java.util.Comparator;

public record EventTime(String event, long time) implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    public static Comparator<EventTime> descTimeDescEventComparator() {
        return Comparator.comparing( EventTime::time ).thenComparing( EventTime::event );
    }

}
