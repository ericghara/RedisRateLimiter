package com.ericgha.service.data;

import com.ericgha.dto.EventTime;

public interface EventMapService {

    boolean putEvent(EventTime event);

    String keyPrefix();

}
