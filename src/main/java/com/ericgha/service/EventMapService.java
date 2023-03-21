package com.ericgha.service;

import com.ericgha.dao.EventMap;
import exception.WriteConflictException;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Service;

@Service
public class EventMapService {

    private final EventMap eventMap;
    private final RetryTemplate retryTemplate;

    public EventMapService(EventMap eventMap, RetryTemplate retryTemplate) {
        this.eventMap = eventMap;
        this.retryTemplate = retryTemplate;
    }

    public boolean tryAddEvent(String event, long timeMilli) throws WriteConflictException {
        return retryTemplate.execute( _context -> eventMap.putEvent( event, timeMilli ) );
    }

    public boolean tryDeleteEvent(String event, long timeMilli) throws WriteConflictException {
        return retryTemplate.execute( _context -> eventMap.deleteEvent( event, timeMilli ));
    }
}
