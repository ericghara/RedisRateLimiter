package com.ericgha.config;

import com.ericgha.dao.EventQueue;
import com.ericgha.dao.StrictlyOnceMap;
import com.ericgha.dto.EventTime;
import com.ericgha.service.data.EventExpiryService;
import com.ericgha.service.data.EventQueueService;
import com.ericgha.service.data.StrictlyOnceMapService;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.retry.support.RetryTemplate;

public class StrictlyOnceEventConfig {

    @Bean
    StrictlyOnceMap strictlyOnceMap(
            @Qualifier("stringLongRedisTemplate") FunctionRedisTemplate<String, Long> template) {
        return new StrictlyOnceMap( template );
    }

    @Bean
    StrictlyOnceMapService eventMapService(StrictlyOnceMap eventMap) {
        return null;
    }

//    @Bean
//    @Qualifier("strictlyOnceEventQueueService")
//    EventQueueService eventQueueService(FunctionRedisTemplate<String, EventTime> redisTemplate,
//                                        @Value("${xyz}") String keyPrefix,
//                                        @Qualifier("eventQueueRetryTemplate") RetryTemplate retryTemplate) {
//        EventQueue eventQueue = new EventQueue( redisTemplate, keyPrefix );
//        return new EventQueueService( eventQueue, retryTemplate );
//    }
//
//    @Bean
//    @Qualifier("strictlyOnceEventExpiryService")
//    EventExpiryService eventExpiryService(@Qualifier("strictlyOnceEventQueueService") EventQueueService eventQueueService) {
//        EventExpiryService expiryService = new EventExpiryService(eventQueueService);
//        eventExpiryService.start()
//    }
//
//    @Bean
//    StrictlyOnceService strictlyOnceService(StrictlyOnceMapService,
//                                            EventExpiryService eventQueueService) {
//
//    }


}
