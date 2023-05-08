package com.ericgha.service;

import com.ericgha.dto.EventTime;
import org.springframework.http.HttpStatus;

/**
 * Rate limiter handles the <strong>intake</strong> intake of events
 */
public interface RateLimiter {

    /**
     *
     * @param event
     * @return Acceptable status codes
     * <ul>
     *     <li>507 InsufficientStorage: server cannot accept event b/c it's under heavy load and the max limit of concurrent events it can manage is exceeded</li>
     *     <li>503 ServiceUnavailable: if retries were exhausted on a conditional write, etc.</li>
     *     <li>409 conflict: the event cannot be accepted b/c the rate limit for the event has been exceeded</li>
     *     <li>201 created: the event has been accepted</li>
     * </ul>
     */
    HttpStatus acceptEvent(EventTime event);

}
