package com.ericgha.dto;

import java.io.Serial;
import java.io.Serializable;

public record EventTime(String event, long time) implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

}
