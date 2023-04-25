package com.ericgha.dto;

import org.springframework.lang.Nullable;

import java.util.Objects;

public class PollResponse {

    private final Versioned<EventTime> versionedEventTime;
    private final long queueSize;

    public PollResponse(@Nullable Versioned<EventTime> versionedEventTime, long queueSize) {
        this.versionedEventTime = versionedEventTime;
        this.queueSize = queueSize;
    }

    public PollResponse(long queueSize) {
        this( null, queueSize );
    }

    public long queueSize() {
        return this.queueSize;
    }

    @Nullable
    public Versioned<EventTime> versionedEventTime() {
        return this.versionedEventTime;
    }

    public boolean isEmpty() {
        return Objects.nonNull(this.versionedEventTime);
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof PollResponse other) {
            return this.queueSize == other.queueSize() && this.versionedEventTime == other.versionedEventTime();
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Long.hashCode( this.queueSize ) * 31 + Objects.hashCode( this.versionedEventTime);
    }

    @Override public String toString() {
        return "PollResponse{" +
                "versionedEventTime=" + versionedEventTime +
                ", queueSize=" + queueSize +
                '}';
    }
}
