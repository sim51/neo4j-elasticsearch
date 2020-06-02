package org.neo4j.elasticsearch.test;

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
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.harness.junit.rule.Neo4jRule;
import org.neo4j.elasticsearch.ElasticSearchKernelExtensionFactory;
import org.neo4j.elasticsearch.ElasticSearchProcedures;
import org.neo4j.elasticsearch.config.ElasticSearchConfig;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

import java.util.List;
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
        ElasticSearchConfig esConfig = ElasticSearchConfig.forDatabase(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
        neo4j = new Neo4jRule()
                .withProcedure(ElasticSearchProcedures.class)
                .withExtensionFactories(List.of(new ElasticSearchKernelExtensionFactory()))
                .withConfig(esConfig.CONFIG_ES_URL, "http://" + container.getHttpHostAddress())
                .withConfig(esConfig.CONFIG_ES_INDEX_SPEC, index + ":" + label + "(" + String.join(", ", properties) + ")")
                .withConfig(esConfig.CONFIG_ES_USE_INDEX_TYPE, Boolean.FALSE)
                .withConfig(esConfig.CONFIG_ES_INCLUDE_DB, Boolean.TRUE)
                .withConfig(esConfig.CONFIG_ES_ASYNC, Boolean.FALSE);
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

    public static String getEsNodeId(Node node) {
        return GraphDatabaseSettings.DEFAULT_DATABASE_NAME + "_" + String.valueOf(node.getId());
    }

    public Driver getNeo4jDriver() {
        return GraphDatabase.driver(neo4j.boltURI(), Config.builder().withoutEncryption().build());
    }

    public Node createNode(String label, Map<String, String> properties) {
        try (Transaction tx = neo4j.defaultDatabaseService().beginTx()) {
            Node node = tx.createNode(Label.label(label));
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                node.setProperty(entry.getKey(), entry.getValue());
            }
            tx.commit();
            ;
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
