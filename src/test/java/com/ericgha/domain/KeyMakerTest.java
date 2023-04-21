package com.ericgha.domain;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class KeyMakerTest {

    static final String KEY_PREFIX = "TEST";
    KeyMaker keyMaker = new KeyMaker( KEY_PREFIX );

    @Test
    void generateClockKeyReturnsExpected() {
        String expected = KEY_PREFIX + KeyMaker.KEY_DELIMITER + KeyMaker.CLOCK_IDENTIFIER;
        String found = keyMaker.generateClockKey();
        Assertions.assertEquals( expected, found );
    }

    @Test
    void generateQueueKeyReturnsExpected() {
        String expected = KEY_PREFIX + KeyMaker.KEY_DELIMITER + KeyMaker.CLOCK_IDENTIFIER;
        String found = keyMaker.generateClockKey();
        Assertions.assertEquals( expected, found );
    }

    @Test
    void generateEventKeyReturnsExpected() {
        String event = "test event";
        String expected =
                KEY_PREFIX + KeyMaker.KEY_DELIMITER + KeyMaker.EVENT_IDENTIFIER + KeyMaker.KEY_DELIMITER + event;
        String found = keyMaker.generateEventKey( event );
        Assertions.assertEquals( expected, found );
    }
}