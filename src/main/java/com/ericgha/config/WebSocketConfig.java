package com.ericgha.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.simp.config.MessageBrokerRegistry;
import org.springframework.web.socket.config.annotation.EnableWebSocketMessageBroker;
import org.springframework.web.socket.config.annotation.StompEndpointRegistry;
import org.springframework.web.socket.config.annotation.WebSocketMessageBrokerConfigurer;

@Configuration
@EnableWebSocketMessageBroker
public class WebSocketConfig implements WebSocketMessageBrokerConfigurer {

    @Value("${app.web-socket.prefix.application}")
    private String applicationPrefix;
    @Value("${app.web-socket.prefix.client}")
    private String clientPrefix;


    @Override public void registerStompEndpoints(StompEndpointRegistry registry) {
        registry.addEndpoint( "/ws" )
                .setAllowedOrigins( "*" ); // todo remove this
    }

    @Override public void configureMessageBroker(MessageBrokerRegistry registry) {
        registry.setApplicationDestinationPrefixes( applicationPrefix );
        registry.enableSimpleBroker( clientPrefix );
    }
}
