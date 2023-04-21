package com.ericgha.config;

import com.ericgha.dto.EventTime;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisPassword;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.jedis.JedisClientConfiguration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.serializer.GenericToStringSerializer;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

@Configuration
public class RedisConfig {

    @Value("classpath:${app.redis.functions-resource}")
    Resource redisFunctions;

    @Bean
    Jackson2JsonRedisSerializer<EventTime> jackson2JsonRedisSerializer() {
        ObjectMapper om = new ObjectMapper();
        om.setVisibility( PropertyAccessor.ALL, JsonAutoDetect.Visibility.ANY );
        return new Jackson2JsonRedisSerializer<>( om, EventTime.class );
    }

    @Bean
    StringRedisSerializer stringRedisSerializer() {
        return new StringRedisSerializer();
    }


    @Bean
    @ConditionalOnProperty(name = "app.redis.disable-bean.redis-connection-factory", havingValue = "false",
            matchIfMissing = true)
    public RedisConnectionFactory redisConnectionFactory(@Value("${spring.data.redis.host}") String redisHostname,
                                                         @Value("${spring.data.redis.password}") String password,
                                                         @Value("${spring.data.redis.port}")
                                                         Integer redisPort) {
        RedisPassword redisPassword = password.isBlank() ? RedisPassword.none() : RedisPassword.of( password );
        RedisStandaloneConfiguration config = new RedisStandaloneConfiguration( redisHostname, redisPort );
        config.setPassword( redisPassword );
        JedisClientConfiguration clientConfiguration = JedisClientConfiguration.builder()
                .clientName( redisHostname )
                .build();
        return new JedisConnectionFactory( config, clientConfiguration );
    }

    @Bean
    @Qualifier("stringTemplate")
    @ConditionalOnProperty(name = "app.redis.disable-bean.string-redis-template", havingValue = "false",
            matchIfMissing = true)
    FunctionRedisTemplate<String, String> stringTemplate(RedisConnectionFactory redisConnectionFactory,
                                                         StringRedisSerializer stringRedisSerializer) {
        FunctionRedisTemplate<String, String> template = new FunctionRedisTemplate<>( redisFunctions );
        // set serializers
        template.setKeySerializer( stringRedisSerializer );
        template.setValueSerializer( stringRedisSerializer );
        template.setHashKeySerializer( stringRedisSerializer );
        template.setHashValueSerializer( stringRedisSerializer );

        template.setConnectionFactory( redisConnectionFactory );
        template.setEnableTransactionSupport( true );
        template.afterPropertiesSet();
        return template;
    }

    @Bean
    @Qualifier("eventTimeTemplate")
    @ConditionalOnProperty(name = "app.redis.disable-bean.event-time-redis-template", havingValue = "false",
            matchIfMissing = true)
    FunctionRedisTemplate<String, EventTime> eventTimeTemplate(RedisConnectionFactory redisConnectionFactory,
                                                               StringRedisSerializer stringRedisSerializer,
                                                               Jackson2JsonRedisSerializer<EventTime> jackson2JsonRedisSerializer) {
        FunctionRedisTemplate<String, EventTime> template = new FunctionRedisTemplate<>( redisFunctions );
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

    @Bean
    @Qualifier("stringLongTemplate")
    @ConditionalOnProperty(name = "app.redis.disable-bean.string-long-redis-template", havingValue = "false",
            matchIfMissing = true)
    FunctionRedisTemplate<String, Long> stringLongTemplate(RedisConnectionFactory redisConnectionFactory,
                                                                StringRedisSerializer stringRedisSerializer) {
        FunctionRedisTemplate<String, Long> template = new FunctionRedisTemplate<>( redisFunctions );
        GenericToStringSerializer<Long> longSerializer = new GenericToStringSerializer<>( Long.class );
        // keys
        template.setKeySerializer( stringRedisSerializer );
        template.setHashKeySerializer( stringRedisSerializer );
        // values
        template.setValueSerializer( longSerializer );
        template.setHashValueSerializer( longSerializer );
        // etc
        template.setConnectionFactory( redisConnectionFactory );
        template.setEnableTransactionSupport( true );
        template.afterPropertiesSet();
        return template;
    }
}