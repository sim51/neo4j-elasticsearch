package org.neo4j.elasticsearch;

import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import org.junit.Test;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.elasticsearch.test.ElasticSearchIntegrationTest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ElasticSearchProceduresTest extends ElasticSearchIntegrationTest {

    @Test
    public void procedure_indexall_should_work() throws Exception {
        Driver driver = getNeo4jDriver();

        // Load some data into Neo4j
        try (Session session = driver.session()) {
            session.run("UNWIND range(1, 101, 1) AS index CREATE (:MyLabel { foo: 'foo_' + index}) RETURN count(*) AS count");
        }

        // Reset the index
        resetIndex();

        // Run the procedure
        try (Session session = driver.session()) {
            Result rs = session.run("CALL elasticsearch.indexAll({ batchSize:50, async:false });");

            // Check the return of the procedure
            Record record = rs.single();
            assertEquals(3, record.get("numberOfBatches").asInt());
            assertEquals(101, record.get("numberOfIndexedDocument").asInt());
        }

        // Check the index
        String query = "{\n" +
                "  \"query\": {\n" +
                "    \"match\": {\n" +
                "      \"@labels\": \"MyLabel\"\n" +
                "    }\n" +
                "  }\n" +
                "}";

        // Wait for the elasticsearch refresh
        Thread.sleep(2000);

        try (JestClient client = getJestClient()) {
            SearchResult result = client.execute(new Search.Builder(query).build());
            assertTrue(result.getErrorMessage(), result.isSucceeded());
            assertEquals(101, result.getJsonObject().get("hits").getAsJsonObject().get("total").getAsJsonObject().get("value").getAsInt());
        }

    }

}
