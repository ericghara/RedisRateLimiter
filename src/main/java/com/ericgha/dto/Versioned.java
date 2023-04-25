package com.ericgha.dto;

import org.springframework.lang.NonNull;

/**
 * A generic container for versioned data.  The {@code clock} field allows {@code Versioned} objects (versioned to
 * the same clock) to be linearized.
 * @param <T>
 */
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

    @Override
    public boolean equals(Object other) {
        if (other instanceof Versioned<?> otherVersioned) {
            return otherVersioned.clock == this.clock && otherVersioned.data.equals(this.data);
        }
        return false;
    }

    @Override
    public String toString() {
        return String.format("Versioned{clock=%d, data=%s}", clock, data);
    }

    @Override
    public int hashCode() {
        int hash = Long.hashCode( clock );
        return hash * 31 + data().hashCode();
    }


}
