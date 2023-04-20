package com.ericgha.dto;

import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;

import java.util.Objects;

public class TimeIsValidDiff {

    private final TimeIsValid previous;
    private final TimeIsValid current;
    private final Long curVersion;

    public TimeIsValidDiff(@NonNull TimeIsValid previous, @NonNull TimeIsValid current, @Nullable Long curVersion) {
        this.previous = previous;
        this.current = current;
        this.curVersion = curVersion;
    }

    public TimeIsValid previous() {
        return this.previous;
    }

    public TimeIsValid current() {
        return this.current;
    }

    @Nullable
    public Long currentVersion() {
        return this.curVersion;
    }

    @Override public String toString() {
        return "TimeIsValidDiff{" +
                "previous=" + previous +
                ", current=" + current +
                ", curVersion=" + curVersion +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o instanceof TimeIsValidDiff other) {
            return Objects.equals( this.curVersion, other.curVersion ) && this.current.equals( other.current ) &&
                    this.previous.equals( other.previous );
        }
        return false;
    }

    @Override public int hashCode() {
        int result = previous.hashCode();
        result = 31 * result + current.hashCode();
        result = 31 * result + ( curVersion != null ? curVersion.hashCode() : 0 );
        return result;
    }
}
