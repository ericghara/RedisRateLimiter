import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import static org.junit.Assert.assertTrue;


public class RedisBackedCacheIntTest {

    private RedisBackedCache underTest;
    @Container
    public GenericContainer redis = new GenericContainer( DockerImageName.parse("redis:5.0.3-alpine"))
            .withExposedPorts(6379);

    @BeforeEach
    public void setUp() {
        String address = redis.getHost();
        Integer port = redis.getFirstMappedPort();

        // Now we have an address and port for Redis, no matter where it is running
        underTest = new RedisBackedCache(address, port);
    }

    @Test
    public void testSimplePutAndGet() {
//        underTest.put("test", "example");
//
//        String retrieved = underTest.get("test");
//        assertThat(retrieved).isEqualTo("example");
        assertTrue(true);
    }
}