package com.ericgha.test_fixtures;

import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.core.env.MapPropertySource;
import org.springframework.test.context.ContextConfigurationAttributes;
import org.springframework.test.context.ContextCustomizer;
import org.springframework.test.context.ContextCustomizerFactory;
import org.springframework.test.context.MergedContextConfiguration;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.List;
import java.util.Map;

public class RedisTestContainerContextCustomizerFactory implements ContextCustomizerFactory {

    @Override
    public ContextCustomizer createContextCustomizer(Class<?> testClass,
                                                     List<ContextConfigurationAttributes> configAttributes) {
        if (!( AnnotatedElementUtils.hasAnnotation( testClass, EnableRedisTestContainer.class ) )) {
            return null;
        }
        return new RedisTestContainerContextCustomizer();
    }

    private static class RedisTestContainerContextCustomizer implements ContextCustomizer, AutoCloseable {

        GenericContainer<?> redisContainer = new GenericContainer<>( DockerImageName.parse( "redis:7" ) )
                .withExposedPorts( 6379 )
                .withReuse( true );

        @Override
        public void customizeContext(ConfigurableApplicationContext context, MergedContextConfiguration mergedConfig) {
            redisContainer.start();
            var properties = Map.<String, Object>of(
                    "spring.data.redis.host", redisContainer.getHost(),
                    "spring.data.redis.port", redisContainer.getMappedPort( 6379 ),
                    "spring.data.redis.password", ""
            );
            var propertySource = new MapPropertySource( "RedisContainer Test Properties", properties );
            context.getEnvironment().getPropertySources().addFirst( propertySource );
        }

        @Override
        // required by ContextCustomizer interface (see docs)
        public boolean equals(Object other) {
            return other instanceof RedisTestContainerContextCustomizer;
        }

        @Override
        // required by ContextCustomizer interface (see docs)
        public int hashCode() {
            return redisContainer.hashCode();
        }

        @Override public void close() {
            redisContainer.stop();
        }
    }
}
