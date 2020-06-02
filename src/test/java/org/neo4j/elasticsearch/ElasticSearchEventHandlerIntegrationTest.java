package org.neo4j.elasticsearch;

import org.neo4j.elasticsearch.test.ElasticSearchIntegrationTest;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.core.Get;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import org.junit.Test;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Session;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import java.util.HashMap;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ElasticSearchEventHandlerIntegrationTest extends ElasticSearchIntegrationTest {


    @Test
    public void es_indexation_of_a_created_node_should_work() throws Exception {
        // Create a Neo4j node
        Node node = createNode(label, new HashMap<String, String>() {{
            put("foo", "bar");
            put("hello", "world");
        }});

        // Request to ES
        try (JestClient client = getJestClient()) {
            JestResult response = client.execute(new Get.Builder(index, String.valueOf(node.getId())).refresh(true).build());

            // Check the response
            assertEquals(true, response.isSucceeded());
            assertEquals(index, response.getValue("_index"));
            assertEquals(String.valueOf(node.getId()), response.getValue("_id"));
            assertEquals("_doc", response.getValue("_type"));

            // Check the response's content
            Map source = response.getSourceAsObject(Map.class);
            assertEquals(asList(label), source.get("labels"));
            assertEquals(String.valueOf(node.getId()), source.get("id"));
            assertEquals("bar", source.get("foo"));
            assertEquals("world", source.get("hello"));
        }
    }

    @Test
    public void es_desindexation_of_a_deleted_node_should_work() throws Exception {
        // Create a Neo4j node
        Node nodeCreated = createNode(label, new HashMap<String, String>() {{
            put("foo", "bar");
            put("hello", "world");
        }});

        // Delete the node
        try (Transaction tx = neo4j.getGraphDatabaseService().beginTx()) {
            Node node = neo4j.getGraphDatabaseService().getNodeById(nodeCreated.getId());
            node.delete();
            tx.success();
        }

        // Request to ES
        try (JestClient client = getJestClient()) {
            JestResult response = client.execute(new Get.Builder(index, String.valueOf(nodeCreated.getId())).refresh(true).build());

            // Check the response's content
            assertEquals(false, response.isSucceeded());
        }
    }

    @Test
    public void es_indexation_of_an_updated_property_should_work() throws Exception {
        // Create a Neo4j node
        Node nodeCreated = createNode(label, new HashMap<String, String>() {{
            put("foo", "bar");
            put("hello", "world");
        }});

        // Update the node
        try (Transaction tx = neo4j.getGraphDatabaseService().beginTx()) {
            Node node = neo4j.getGraphDatabaseService().getNodeById(nodeCreated.getId());
            node.setProperty("foo", "PMU");
            tx.success();
        }

        // Request to ES
        try (JestClient client = getJestClient()) {
            JestResult response = client.execute(new Get.Builder(index, String.valueOf(nodeCreated.getId())).refresh(true).build());

            // Check the response
            assertEquals(true, response.isSucceeded());
            assertEquals(index, response.getValue("_index"));
            assertEquals(String.valueOf(nodeCreated.getId()), response.getValue("_id"));
            assertEquals("_doc", response.getValue("_type"));

            // Check the response's content
            Map source = response.getSourceAsObject(Map.class);
            assertEquals("PMU", source.get("foo"));
            assertEquals("world", source.get("hello"));
        }
    }

    @Test
    public void es_indexation_of_a_removed_property_should_work() throws Exception {
        // Create a Neo4j node
        Node nodeCreated = createNode(label, new HashMap<String, String>() {{
            put("foo", "bar");
            put("hello", "world");
        }});

        // Update the node
        try (Transaction tx = neo4j.getGraphDatabaseService().beginTx()) {
            Node node = neo4j.getGraphDatabaseService().getNodeById(nodeCreated.getId());
            node.removeProperty("hello");
            tx.success();
        }

        // Request to ES
        try (JestClient client = getJestClient()) {
            JestResult response = client.execute(new Get.Builder(index, String.valueOf(nodeCreated.getId())).refresh(true).build());

            // Check the response
            assertEquals(true, response.isSucceeded());
            assertEquals(index, response.getValue("_index"));
            assertEquals(String.valueOf(nodeCreated.getId()), response.getValue("_id"));
            assertEquals("_doc", response.getValue("_type"));

            // Check the response's content
            Map source = response.getSourceAsObject(Map.class);
            assertEquals("bar", source.get("foo"));
            assertEquals(false, source.containsKey("hello"));
        }
    }

    @Test
    public void es_indexation_of_a_removed_property_that_is_the_last_indexed_field_should_work() throws Exception {
        // Create a Neo4j node
        Node nodeCreated = createNode(label, new HashMap<String, String>() {{
            put("foo", "bar");
        }});

        // Update the node
        try (Transaction tx = neo4j.getGraphDatabaseService().beginTx()) {
            Node node = neo4j.getGraphDatabaseService().getNodeById(nodeCreated.getId());
            node.removeProperty("foo");
            tx.success();
        }

        // Request to ES
        try (JestClient client = getJestClient()) {
            JestResult response = client.execute(new Get.Builder(index, String.valueOf(nodeCreated.getId())).refresh(true).build());

            // Check the response's content
            assertEquals(false, response.isSucceeded());
        }
    }

    @Test
    public void es_indexation_of_an_added_label_should_work() throws Exception {
        // Create a Neo4j node
        Node nodeCreated = createNode("Test", new HashMap<String, String>() {{
            put("foo", "bar");
            put("hello", "world");
        }});

        // Add labelon  the node
        try (Transaction tx = neo4j.getGraphDatabaseService().beginTx()) {
            Node node = neo4j.getGraphDatabaseService().getNodeById(nodeCreated.getId());
            node.addLabel(Label.label(label));
            tx.success();
        }

        // Request to ES
        try (JestClient client = getJestClient()) {
            JestResult response = client.execute(new Get.Builder(index, String.valueOf(nodeCreated.getId())).refresh(true).build());

            // Check the response
            assertEquals(true, response.isSucceeded());
            assertEquals(index, response.getValue("_index"));
            assertEquals(String.valueOf(nodeCreated.getId()), response.getValue("_id"));
            assertEquals("_doc", response.getValue("_type"));

            // Check the response's content
            Map source = response.getSourceAsObject(Map.class);
            assertEquals("bar", source.get("foo"));
            assertEquals("world", source.get("hello"));
        }
    }

    @Test
    public void es_desindexation_of_a_removed_label_should_work() throws Exception {
        // Create a Neo4j node
        Node nodeCreated = createNode(label, new HashMap<String, String>() {{
            put("foo", "bar");
            put("hello", "world");
        }});

        // Delete the label
        try (Transaction tx = neo4j.getGraphDatabaseService().beginTx()) {
            Node node = neo4j.getGraphDatabaseService().getNodeById(nodeCreated.getId());
            node.removeLabel(Label.label(label));
            tx.success();
        }

        // Request to ES
        try (JestClient client = getJestClient()) {
            JestResult response = client.execute(new Get.Builder(index, String.valueOf(nodeCreated.getId())).refresh(true).build());

            // Check the response's content
            assertEquals(false, response.isSucceeded());
        }
    }

    @Test
    public void es_desindexation_should_work_on_2d_point() throws Exception {
        createMappingFor("{\n" +
                "  \"properties\": {\n" +
                "    \"foo\": {\n" +
                "      \"type\": \"geo_point\"\n" +
                "    }\n" +
                "  }\n" +
                "}");

        Driver driver = getNeo4jDriver();


        // Load some data into Neo4j
        try (Session session = driver.session()) {
            session.run("CREATE (n:MyLabel { foo:point({x: 1, y: 2}) })");
        }

        // Wait for the elasticsearch refresh
        Thread.sleep(1000);

        // Request to ES
        String query = "{\n" +
                "  \"query\": {\n" +
                "    \"geo_bounding_box\": { \n" +
                "      \"foo\": {\n" +
                "        \"top_left\": {\n" +
                "          \"lat\": 2,\n" +
                "          \"lon\": 0\n" +
                "        },\n" +
                "        \"bottom_right\": {\n" +
                "          \"lat\": 0,\n" +
                "          \"lon\": 2\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";

        try (JestClient client = getJestClient()) {
            SearchResult result = client.execute(new Search.Builder(query).build());
            assertTrue(result.getErrorMessage(), result.isSucceeded());
            assertEquals(1, result.getJsonObject().get("hits").getAsJsonObject().get("total").getAsJsonObject().get("value").getAsInt());
        }
    }

    @Test
    public void es_desindexation_should_work_on_3d_point() throws Exception {
        // /!\ We scan store a 3d point, but the ES index is only done on the 2d point
        Driver driver = getNeo4jDriver();
        createMappingFor("{\n" +
                "  \"properties\": {\n" +
                "    \"foo\": {\n" +
                "      \"type\": \"geo_point\"\n" +
                "    }\n" +
                "  }\n" +
                "}");

        // Load some data into Neo4j
        try (Session session = driver.session()) {
            session.run("CREATE (n:MyLabel { foo: point({x: 1, y: 2, z:3}) })");
        }

        // Wait for the elasticsearch refresh
        Thread.sleep(1000);

        // Request to ES
        String query = "{\n" +
                "  \"query\": {\n" +
                "    \"geo_bounding_box\": { \n" +
                "      \"foo\": {\n" +
                "        \"top_left\": {\n" +
                "          \"lat\": 2,\n" +
                "          \"lon\": 0\n" +
                "        },\n" +
                "        \"bottom_right\": {\n" +
                "          \"lat\": 0,\n" +
                "          \"lon\": 2\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";

        try (JestClient client = getJestClient()) {
            SearchResult result = client.execute(new Search.Builder(query).build());
            assertTrue(result.getErrorMessage(), result.isSucceeded());
            assertEquals(1, result.getJsonObject().get("hits").getAsJsonObject().get("total").getAsJsonObject().get("value").getAsInt());
        }
    }

    @Test
    public void es_desindexation_should_work_on_date() throws Exception {
        Driver driver = getNeo4jDriver();

        // Load some data into Neo4j
        try (Session session = driver.session()) {
            session.run("CREATE (n:MyLabel { foo: date(\"1983-03-26\") })");
        }

        // Wait for the elasticsearch refresh
        Thread.sleep(1000);

        // Request to ES
        String query = "{\n" +
                "    \"query\": {\n" +
                "        \"range\" : {\n" +
                "            \"foo\" : {\n" +
                "                \"gte\" : \"1983-03-25\",\n" +
                "                \"lte\" : \"1983-03-27\" \n" +
                "            }\n" +
                "        }\n" +
                "    }\n" +
                "}";

        try (JestClient client = getJestClient()) {
            SearchResult result = client.execute(new Search.Builder(query).build());
            assertTrue(result.getErrorMessage(), result.isSucceeded());
            assertEquals(1, result.getJsonObject().get("hits").getAsJsonObject().get("total").getAsJsonObject().get("value").getAsInt());
        }
    }

    @Test
    public void es_desindexation_should_work_on_datetime() throws Exception {
        Driver driver = getNeo4jDriver();

        // Load some data into Neo4j
        try (Session session = driver.session()) {
            session.run("CREATE (n:MyLabel { foo: datetime(\"1983-03-26T12:45:30.25[Europe/Berlin]\")})");
        }

        // Wait for the elasticsearch refresh
        Thread.sleep(1000);

        // Request to ES
        String query = "{\n" +
                "    \"query\": {\n" +
                "        \"range\" : {\n" +
                "            \"foo\" : {\n" +
                "                \"gte\" : \"1983-03-26T00:00:00.000Z\",\n" +
                "                \"lte\" : \"1983-03-26T20:00:00.000Z\" \n" +
                "            }\n" +
                "        }\n" +
                "    }\n" +
                "}";

        try (JestClient client = getJestClient()) {
            SearchResult result = client.execute(new Search.Builder(query).build());
            assertTrue(result.getErrorMessage(), result.isSucceeded());
            assertEquals(1, result.getJsonObject().get("hits").getAsJsonObject().get("total").getAsJsonObject().get("value").getAsInt());
        }
    }

}
