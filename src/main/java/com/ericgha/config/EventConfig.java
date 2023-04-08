package com.ericgha.config;

import com.ericgha.service.data.EventExpiryService;
import com.ericgha.service.data.EventQueueService;
import com.ericgha.service.event_consumer.EventConsumer;
import com.ericgha.service.event_consumer.NoOpEventConsumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class EventConfig {

    @Bean
    EventExpiryService eventExpiryService(
            @Value("${app.event-duration-millis}") int eventDuration,
            @Value("${app.event-queue.num-workers}") int numWorkers, @Autowired EventQueueService eventQueueService) {
        EventConsumer eventConsumer = new NoOpEventConsumer(); // todo change to something that sends STOMP messages
        EventExpiryService expiryService = new EventExpiryService( eventQueueService );
        expiryService.start( eventConsumer, eventDuration, numWorkers );
        return expiryService;
    }

}
