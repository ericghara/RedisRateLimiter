package com.ericgha.dto;

import org.springframework.lang.NonNull;

public class Versioned<T> {

    private final long clock;
    private final T data;

    public Versioned(long clock, @NonNull T data) {
        this.clock = clock;
        this.data  = data;
    }

    public long clock() {
        return this.clock;
    }

    public T data() {
        return this.data;
    }


}
