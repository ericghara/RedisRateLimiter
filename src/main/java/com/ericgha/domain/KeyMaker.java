package com.ericgha.domain;

import org.springframework.lang.NonNull;

import java.util.Objects;

/**
 * Builds keys (for key value pairs) where all keys are children of a given prefix.  Intended to allow grouping and organization
 * of keys for example in map data structures.
 * <p>
 * Repeated duplicate calls to {@code KeyMaker} should be avoided.  Clients should instead cache results.
 */
public class KeyMaker {

    public static final String KEY_DELIMITER = ":";
    public static final String QUEUE_IDENTIFIER = "QUEUE";
    public static final String EVENT_IDENTIFIER = "EVENT";
    public static final String CLOCK_IDENTIFIER = "CLOCK";

    private final String keyPrefix;

    /**
     *
     * @param keyPrefix parent which all keys made by this should be a child of.
     */
    public KeyMaker(@NonNull String keyPrefix) {
        Objects.requireNonNull(keyPrefix);
        this.keyPrefix = keyPrefix;
    }

    /**
     * Combine elements to make a key.  Elements will be separated by {@code KEY_DELIMITER}.
     * <p>
     * ex. {@code elements={"a","b","c"} KEY_DELIMITER = ":"} result: {@code "a:b:c"}
     * @param elements
     * @return a string.
     */
    public String encodeKey(String... elements) {
        String[] prefixAndElements = new String[elements.length + 1];
        prefixAndElements[0] = keyPrefix;
        System.arraycopy( elements, 0, prefixAndElements, 1, elements.length );
        return String.join( KEY_DELIMITER, prefixAndElements );
    }

    /**
     * Generates a clock key by adding {@code CLOCK_IDENTIFIER} to the {@code keyPrefix}.
     * @return
     */
    public String generateClockKey() {
        return encodeKey(CLOCK_IDENTIFIER);
    }

    /**
     * Generates a queue key by adding {@code QUEUE_IDENTIFIER} to the {@code keyPrefix}
     * @return
     */
    public String generateQueueKey() {
        return encodeKey(QUEUE_IDENTIFIER);
    }

    /**
     * gernerates an event with the format {@code $keyPrefix:$EVENT_IDENTIFIER:$event} for
     * a {@code KEY_DELIMITER=":"}
     * @param event
     * @return
     */
    public String generateEventKey(@NonNull String event) {
        Objects.requireNonNull( event );
        return encodeKey(EVENT_IDENTIFIER, event);
    }

    /**
     * The key prefix is the prefix which all keys made by {@link KeyMaker} will be children of.
     * @return
     */
    public String keyPrefix() {
        return this.keyPrefix;
    }
}
