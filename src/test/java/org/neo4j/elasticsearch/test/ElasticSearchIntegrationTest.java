package org.neo4j.elasticsearch.test;

import org.neo4j.elasticsearch.ElasticSearchProcedures;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.DeleteIndex;
import io.searchbox.indices.mapping.PutMapping;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.harness.junit.Neo4jRule;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

import java.util.Map;

public abstract class ElasticSearchIntegrationTest {

    // Name of the ES index (mainly for it's creation)
    public final static String index = "my_index";
    // Label
    public final static String label = "MyLabel";
    // Properties
    public final static String[] properties = new String[]{"foo", "hello"};

    @ClassRule
    public final static ElasticsearchContainer container = new ElasticsearchContainer("docker.elastic.co/elasticsearch/elasticsearch:7.6.0");

    @Rule
    public Neo4jRule neo4j;

    public ElasticSearchIntegrationTest() {
        neo4j = new Neo4jRule()
                .withProcedure(ElasticSearchProcedures.class)
                .withConfig("elasticsearch.host_name", "http://" + container.getHttpHostAddress())
                .withConfig("elasticsearch.index_spec", index + ":" + label + "(" + String.join(",  ", properties) + ")")
                .withConfig("elasticsearch.type_mapping", "false")
                .withConfig("elasticsearch.async", "false");
    }

    public static JestClient getJestClient() {
        // Create ES JEST client
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig
                .Builder("http://" + container.getHttpHostAddress())
                .build());
        return factory.getObject();
    }

    public static void resetIndex() throws Exception {
        try (JestClient client = getJestClient();) {
            client.execute(new DeleteIndex.Builder(index).build());
            client.execute(new CreateIndex.Builder(index).build());
        }
    }

    public static void createMappingFor(String mapping) throws Exception {
        try (JestClient client = getJestClient();) {
            PutMapping putMapping = new PutMapping.Builder(
                    index,
                    null,
                    mapping
            ).build();
            client.execute(putMapping);
        }
    }

    public Driver getNeo4jDriver() {
        return GraphDatabase.driver(neo4j.boltURI(), Config.build().withoutEncryption().toConfig());
    }

    public Node createNode(String label, Map<String, String> properties) {
        try (Transaction tx = neo4j.getGraphDatabaseService().beginTx()) {
            Node node = neo4j.getGraphDatabaseService().createNode(Label.label(label));
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                node.setProperty(entry.getKey(), entry.getValue());
            }
            tx.success();
            return node;
        }
    }

    @Before
    public void setUp() throws Exception {
        // Create ES index
        try (JestClient client = getJestClient();) {
            client.execute(new CreateIndex.Builder(index).build());
        }
    }

    @After
    public void tearDown() throws Exception {
        // Reset Neo4j db
        Driver driver = getNeo4jDriver();
        try (Session session = driver.session()) {
            session.run("MATCH (n) DETACH DELETE n");
        }
        // Delete ES index
        try (JestClient client = getJestClient();) {
            client.execute(new DeleteIndex.Builder(index).build());
        }
        driver.close();
    }

}
