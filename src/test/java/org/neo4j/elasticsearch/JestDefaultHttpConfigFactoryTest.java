package org.neo4j.elasticsearch;

import io.searchbox.client.config.HttpClientConfig;
import org.apache.http.auth.AuthScope;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JestDefaultHttpConfigFactoryTest {
    private static HttpClientConfig subject;

    @Before
    public void beforeEach() throws Throwable {
        subject = JestDefaultHttpConfigFactory.getConfigFor("http://localhost:9200", Boolean.TRUE, "user", "password", 0, 0);
    }

    @Test
    public void jest_client_with_valid_host_should_work() {
        Set<String> expected = new HashSet<String>(Arrays.asList("http://localhost:9200"));
        assertEquals(expected, subject.getServerList());
    }

    @Test
    public void jest_client_should_use_multithread() {
        assertTrue(subject.isMultiThreaded());
    }

    @Test
    public void jest_client_should_discover_nodes() {
        assertTrue(subject.isDiscoveryEnabled());
    }

    @Test
    public void jest_client_should_discover_everyone() {
        final Long one = 1L;
        assertEquals(one, subject.getDiscoveryFrequency());
    }

    @Test
    public void jest_client_should_use_minute_unit_for_discovery_frequency() {
        assertEquals(TimeUnit.MINUTES, subject.getDiscoveryFrequencyTimeUnit());
    }

    @Test
    public void jest_client_should_use_http_as_default() {
        assertEquals("http://", subject.getDefaultSchemeForDiscoveredNodes());
    }

    @Test
    public void jest_client_with_https_should_work() throws Throwable {
        subject = JestDefaultHttpConfigFactory.getConfigFor("https://localhost:9200", Boolean.TRUE, null, null, 0, 0);
        assertEquals("https://", subject.getDefaultSchemeForDiscoveredNodes());
    }

    @Test
    public void jest_client_with_auth_should_work() throws Throwable {
        subject = JestDefaultHttpConfigFactory.getConfigFor("https://localhost:9200", Boolean.TRUE, "user", "password", 0, 0);
        assertEquals("user", subject.getCredentialsProvider().getCredentials(AuthScope.ANY).getUserPrincipal().getName());
        assertEquals("password", subject.getCredentialsProvider().getCredentials(AuthScope.ANY).getPassword());
    }
}
