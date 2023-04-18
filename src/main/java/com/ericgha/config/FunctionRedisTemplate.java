package com.ericgha.config;

import org.springframework.core.io.Resource;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.RedisTemplate;
import redis.clients.jedis.Jedis;

import java.io.IOException;

/**
 *  A redis template that loads redis functions before
 *
 * @param <K>
 * @param <V>
 */
public class FunctionRedisTemplate<K,V> extends RedisTemplate<K, V> {

    private final Resource redisFunctionsSrc;

    public FunctionRedisTemplate(Resource redisFunctionsSrc) {
        super();
        this.redisFunctionsSrc = redisFunctionsSrc;
    }

    public Jedis getJedisConnection() throws IllegalStateException {
        if (this.getConnectionFactory().getConnection().getNativeConnection() instanceof Jedis jedisConnection) {
            return jedisConnection;
        }
        throw new IllegalStateException("FunctionRedisTemplate only compatible with Jedis.");
    }

    private byte[] getResourceByteArr() {
        try {
            return redisFunctionsSrc.getContentAsByteArray();
        } catch (IOException e) {
            throw new IllegalStateException( "Unable to read resource. " + redisFunctionsSrc.getFilename(), e);
        }
    }

    @Override
    public void afterPropertiesSet() {
        super.afterPropertiesSet();
        try (Jedis connection = getJedisConnection()) {
            connection.functionLoadReplace( getResourceByteArr() );
        } catch (DataAccessException e) {
            throw new IllegalStateException( String.format(
                    "A problem occurred loading while loading redis functions from: %s.  Perhaps check the script?",
                    redisFunctionsSrc.getFilename() ), e);
        }
    }
}
