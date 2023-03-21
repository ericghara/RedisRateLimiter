package com.ericgha.config;

import com.ericgha.dto.EventTime;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;

@Configuration
public class RedisConfig {

    @Bean
    public RedisConnectionFactory redisConnectionFactory(@Value("${spring.data.redis.host}") String redisHostname,
                                                         @Value("${spring.data.redis.port}") Integer redisPort) {
        // not currently required as all properties currently are autoconfigurable, but leaving open
        // for future customization.
        RedisStandaloneConfiguration config = new RedisStandaloneConfiguration( redisHostname, redisPort );
        LettuceClientConfiguration clientConfiguration = LettuceClientConfiguration.builder()
                .clientName( redisHostname ).build();
        return new LettuceConnectionFactory( config, clientConfiguration );
    }

    @Bean
    StringRedisTemplate redisTemplate(RedisConnectionFactory redisConnectionFactory) {
        StringRedisTemplate template = new StringRedisTemplate();
        template.setConnectionFactory( redisConnectionFactory );
        template.setEnableTransactionSupport( true );
        template.afterPropertiesSet();
        return template;
    }

    @Bean
    @Qualifier("EventTimeRedisTemplate")
    RedisTemplate<String, EventTime> eventTimeRedisTemplate(RedisConnectionFactory redisConnectionFactory) {
        RedisTemplate<String, EventTime> template = new RedisTemplate<>();
        template.setConnectionFactory( redisConnectionFactory );
        template.setEnableTransactionSupport( true );
        template.afterPropertiesSet();
        return template;
    }
}