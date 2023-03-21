package com.ericgha.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.jedis.JedisClientConfiguration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;

import java.time.Duration;

@Configuration
public class RedisConfig {

    @Bean
    public RedisConnectionFactory redisConnectionFactory(@Value("${spring.data.redis.host}") String redisHostname,
                                                         @Value("${spring.data.redis.port}") Integer redisPort) {
        // not currently required as all properties currently are autoconfigurable, but leaving open
        // for future customization.
        // To switch to jedis remember to switch client type in spring.data.redis.client-type
        RedisStandaloneConfiguration config = new RedisStandaloneConfiguration( redisHostname, redisPort );
        JedisClientConfiguration jedisClientConfiguration = JedisClientConfiguration.builder()
                .clientName( redisHostname ).usePooling().build();
        JedisConnectionFactory connectionFactory = new JedisConnectionFactory( config, jedisClientConfiguration );
        connectionFactory.getPoolConfig().setMaxIdle( 8 );
        connectionFactory.getPoolConfig().setMaxTotal( 8 );
        connectionFactory.getPoolConfig().setMaxWait( Duration.ofSeconds(2) );
        return connectionFactory;
    }

    @Bean
    RedisTemplate<String, String> redisTemplate(RedisConnectionFactory redisConnectionFactory) {
        RedisTemplate<String, String> template = new RedisTemplate<>();
        template.setConnectionFactory( redisConnectionFactory );
        template.setEnableTransactionSupport( true );
        template.setExposeConnection( true );
        template.afterPropertiesSet();
        return template;
    }
}