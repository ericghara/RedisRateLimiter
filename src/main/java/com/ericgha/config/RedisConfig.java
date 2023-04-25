package com.ericgha.config;

import com.ericgha.service.data.FunctionRedisTemplate;
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
import org.springframework.data.redis.serializer.StringRedisSerializer;

@Configuration
public class RedisConfig {

    @Value("classpath:${app.redis.functions-resource}")
    Resource redisFunctions;

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
                .usePooling()
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