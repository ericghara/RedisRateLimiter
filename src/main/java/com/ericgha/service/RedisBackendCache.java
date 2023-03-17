package com.ericgha.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.stereotype.Service;

@Service
public class RedisBackendCache {

    private final RedisTemplate<String, String> redisTemplate;
    private final ValueOperations<String, String> valueOps;

    @Autowired
    RedisBackendCache(RedisTemplate<String,String> redisTemplate) {
        this.redisTemplate = redisTemplate;
        this.valueOps = redisTemplate.opsForValue();
    }

    public void put(String key, String value) {
        valueOps.set(key, value);
    }

    public String get(String key) {
        return valueOps.get(key);
    }

}
