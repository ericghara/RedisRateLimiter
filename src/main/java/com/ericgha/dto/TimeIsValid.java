package com.ericgha.dto;

import org.springframework.lang.Nullable;

import java.util.Objects;

public class TimeIsValid {

    private final Long time;
    private final Boolean isValid;

    public TimeIsValid(Long time, Boolean isValid) {
        this.time = time;
        this.isValid = isValid;
    }

    @Nullable
    public Long time() {
        return this.time;
    }

    @Nullable
    public Boolean isValid() {
        return isValid;
    }

    @Override
    public String toString() {
        return String.format("TimeIsValid{time=%d, isValid=%b}", time, isValid);
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof TimeIsValid other) {
            return Objects.equals(this.time, other.time()) && Objects.equals(this.isValid, other.isValid() );
        }
        return false;
    }

    @Override
    public int hashCode() {
        // Objects.hashCode returns 0 for null and 0 for false...too many potential collisions
        int bHash = Objects.isNull(isValid) ? 0 : 1 + this.isValid.hashCode();
        return Objects.hashCode(this.time) * 31 + bHash;
    }

}
