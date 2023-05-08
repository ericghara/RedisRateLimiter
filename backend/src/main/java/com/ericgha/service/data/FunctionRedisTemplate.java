package com.ericgha.service.data;

import org.springframework.core.io.Resource;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.RedisTemplate;
import redis.clients.jedis.Jedis;

import java.io.IOException;

/**
 * A redis template that handles provisioning of redis functions on {@link FunctionRedisTemplate#afterPropertiesSet()}.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class FunctionRedisTemplate<K, V> extends RedisTemplate<K, V> {

    private final Resource redisFunctionsSrc;

    /**
     * @param redisFunctionsSrc resource file where functions should be loaded from
     */
    public FunctionRedisTemplate(Resource redisFunctionsSrc) {
        super();
        this.redisFunctionsSrc = redisFunctionsSrc;
    }

    /**
     * A convenience function to get the underlying Jedis connection.
     *
     * @return a Jedis connection
     * @throws IllegalStateException
     */
    public Jedis getJedisConnection() throws IllegalStateException {
        if (this.getConnectionFactory().getConnection().getNativeConnection() instanceof Jedis jedisConnection) {
            return jedisConnection;
        }
        throw new IllegalStateException( "FunctionRedisTemplate only compatible with Jedis." );
    }

    private byte[] getResourceByteArr() {
        try {
            return redisFunctionsSrc.getContentAsByteArray();
        } catch (IOException e) {
            throw new IllegalStateException( "Unable to read resource. " + redisFunctionsSrc.getFilename(), e );
        }
    }


    /**
     * @throws IllegalStateException if the underlying connection is not a Jedis connection or if an error occured
     * (up)loading the redis functions.
     */
    @Override
    public void afterPropertiesSet() throws IllegalStateException {
        super.afterPropertiesSet();
        try (Jedis connection = getJedisConnection()) {
            connection.functionLoadReplace( getResourceByteArr() );
        } catch (DataAccessException e) {
            throw new IllegalStateException( String.format(
                    "A problem occurred loading while loading redis functions from: %s.  Perhaps check the script?",
                    redisFunctionsSrc.getFilename() ), e );
        }
    }
}
