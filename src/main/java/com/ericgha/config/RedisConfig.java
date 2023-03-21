package com.ericgha.config;

import com.ericgha.dto.EventTime;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

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
    StringRedisTemplate stringRedisTemplate(RedisConnectionFactory redisConnectionFactory) {
        StringRedisTemplate template = new StringRedisTemplate();
        template.setConnectionFactory( redisConnectionFactory );
        template.setEnableTransactionSupport( true );
        template.afterPropertiesSet();
        return template;
    }

    @Bean
    Jackson2JsonRedisSerializer<EventTime> jackson2JsonRedisSerializer() {
        ObjectMapper om = new ObjectMapper();
        om.setVisibility( PropertyAccessor.ALL, JsonAutoDetect.Visibility.ANY);
        return new Jackson2JsonRedisSerializer<>(om, EventTime.class);
    }

    @Bean
    StringRedisSerializer stringRedisSerializer() {
        return new StringRedisSerializer();
    }

    @Bean
    @Qualifier("EventTimeRedisTemplate")
    RedisTemplate<String, EventTime> eventTimeRedisTemplate(RedisConnectionFactory redisConnectionFactory,
                                                            StringRedisSerializer stringRedisSerializer,
                                                            Jackson2JsonRedisSerializer<EventTime> jackson2JsonRedisSerializer) {
        RedisTemplate<String, EventTime> template = new RedisTemplate<>();
        template.setConnectionFactory( redisConnectionFactory );
        // keys use string serializer
        template.setKeySerializer( stringRedisSerializer );
        template.setHashKeySerializer( stringRedisSerializer );
        // values use json serializer
        template.setValueSerializer( jackson2JsonRedisSerializer );
        template.setHashValueSerializer( jackson2JsonRedisSerializer );
        template.setEnableTransactionSupport( true );
        template.afterPropertiesSet();
        return template;
    }
}