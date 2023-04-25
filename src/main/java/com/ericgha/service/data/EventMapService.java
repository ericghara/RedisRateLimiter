package com.ericgha.service.data;

import com.ericgha.dto.EventTime;

/**
 * An interface for event maps.
 */
public interface EventMapService {

    boolean putEvent(EventTime event);

    String keyPrefix();

}
