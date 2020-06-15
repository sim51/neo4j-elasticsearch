package org.neo4j.elasticsearch;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.harness.junit.rule.Neo4jRule;

import java.util.List;

import static org.junit.Assert.assertNotNull;

public class ElasticSearchPluginWithoutConfigTest {

        @Rule
    public Neo4jRule neo4j;

    public ElasticSearchPluginWithoutConfigTest() {
        neo4j = new Neo4jRule()
                .withProcedure(ElasticSearchProcedures.class)
                .withExtensionFactories(List.of(new ElasticSearchKernelExtensionFactory()));
    }

    @Test
    public void neo4j_should_start_without_any_es_configuration() throws Exception {
        Node node = null;
        try (Transaction tx = neo4j.defaultDatabaseService().beginTx()) {
            node = tx.createNode(Label.label("JustATest"));
            tx.commit();
        }
        assertNotNull(node);
    }


}
