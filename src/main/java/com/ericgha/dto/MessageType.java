package com.ericgha.dto;

public enum MessageType {

    KEY_FRAME( "KEY-FRAME" ),
    ADDED_EVENT( "ADDED-EVENT" ),
    PUBLISHED_EVENT( "PUBLISHED-EVENT" ),
    INVALIDATED_EVENT( "INVALIDATED-EVENT" );

    final String identifier;

    MessageType(String identifier) {
        this.identifier = identifier;
    }

    public String identifier() {
        return this.identifier;
    }

}
