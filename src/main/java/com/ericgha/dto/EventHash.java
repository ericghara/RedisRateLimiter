package com.ericgha.dto;

import org.springframework.lang.Nullable;

import java.util.Objects;

public class EventHash {

    private final Long time;
    private final Boolean isValid;
    private final Long retired;

    public EventHash(@Nullable Long time, @Nullable Boolean isValid, @Nullable Long retired) throws IllegalArgumentException {
            this.time = time;
            this.isValid = isValid;
            this.retired = retired;
    }

    @Nullable
    public Long time() {
        return this.time;
    }

    @Nullable
    public Boolean isValid() {
        return this.isValid;
    }

    @Nullable
    public Long retired() {
        return this.retired;
    }

    @Override public String toString() {
        return "EventHash{" +
                "time=" + time +
                ", isValid=" + isValid +
                ", retired=" + retired +
                '}';
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o instanceof EventHash other) {
            return Objects.equals( this.time, other.time ) && Objects.equals( this.isValid, other.isValid() ) &&
                    Objects.equals( this.retired, other.retired() );
        }
        return false;
    }

    @Override public int hashCode() {
        int result = Objects.isNull( isValid ) ? 0 : 1 + this.isValid.hashCode();
        result = 31 * result + Objects.hashCode( time );
        result = 31 * result + Objects.hashCode( retired );
        return result;
    }
}
