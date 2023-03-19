package com.ericgha.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;

@Configuration
class RedisConfiguration {

    @Value("${spring.data.redis.host}")
    String redisHostname;
    @Value("${spring.data.redis.port}")
    Integer redisPort;

    @Bean
    public RedisConnectionFactory redisConnectionFactory() {
        // not currently required as all properties currently are autoconfigurable, but leaving open
        // for future customization.
        // To switch to jedis remember to switch client type in spring.data.redis.client-type
        RedisStandaloneConfiguration config = new RedisStandaloneConfiguration(redisHostname, redisPort);
        return new JedisConnectionFactory(config);
    }

    @Bean
    RedisTemplate<String, String> redisTemplate(JedisConnectionFactory redisConnectionFactory) {
        RedisTemplate<String, String> template = new RedisTemplate<>();
        template.setConnectionFactory(redisConnectionFactory);
        return template;
    }
}