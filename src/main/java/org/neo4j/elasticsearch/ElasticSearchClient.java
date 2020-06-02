package org.neo4j.elasticsearch;

import org.neo4j.elasticsearch.config.ElasticSearchConfig;
import org.neo4j.elasticsearch.config.ElasticSearchIndexSpec;
import org.neo4j.elasticsearch.model.DocumentIndexId;
import io.searchbox.action.BulkableAction;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.JestResultHandler;
import io.searchbox.core.Bulk;
import io.searchbox.core.BulkResult;
import io.searchbox.core.Delete;
import io.searchbox.core.Index;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.spatial.Point;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.GeneralSecurityException;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAmount;
import java.time.temporal.TemporalUnit;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ElasticSearchClient implements JestResultHandler<JestResult> {

    // Logger
    private final static Logger logger = Logger.getLogger(ElasticSearchClient.class.getName());
    // JEST Client
    private final JestClient client;

    /**
     * Initialize the ES client, by creating the Jest client.
     */
    public ElasticSearchClient() {
        JestClientFactory factory = new JestClientFactory();
        String esUrl = ElasticSearchConfig.getConfig(ElasticSearchConfig.CONFIG_ES_URL, "http://localhost:9200");
        boolean esDiscovery = ElasticSearchConfig.configIsEnabled(ElasticSearchConfig.CONFIG_ES_DISCOVERY, false);
        String user = ElasticSearchConfig.getConfig(ElasticSearchConfig.CONFIG_ES_USER, null);
        String password = ElasticSearchConfig.getConfig(ElasticSearchConfig.CONFIG_ES_PASSWORD, null);
        Integer connTineout = ElasticSearchConfig.getConfig(ElasticSearchConfig.CONFIG_ES_CONN_TIMEOUT, 3000);
        Integer readTimeout = ElasticSearchConfig.getConfig(ElasticSearchConfig.CONFIG_ES_READ_TIMEOUT, 3000);
        try {
            factory.setHttpClientConfig(JestDefaultHttpConfigFactory.getConfigFor(esUrl, esDiscovery, user, password, connTineout, readTimeout));
            client = factory.getObject();
        } catch (URISyntaxException e) {
            throw new RuntimeException("Bad elastic search url", e);
        } catch (GeneralSecurityException e) {
            throw new RuntimeException("Failed to intialize es client", e);
        }


    }

    /**
     * Create a JEST delete requests
     *
     * @param node The node to delete in indexes
     * @return The ES REST actions to perform
     */
    public static Map<DocumentIndexId, Delete> deleteRequestsAction(Node node) {
        HashMap<DocumentIndexId, Delete> reqs = new HashMap<>();

        // For each node's label, we check if we have to de-index it
        for (Label l : node.getLabels()) {
            if (ElasticSearchConfig.indices().containsKey(l.name())) {

                // We check all the indices definition for the label (yes a node can be in multiple indices)
                for (ElasticSearchIndexSpec spec : ElasticSearchConfig.indices().get(l.name())) {
                    String id = id(node);
                    String indexName = spec.getIndexName();
                    reqs.put(
                            new DocumentIndexId(indexName, id),
                            new Delete.Builder(id).index(indexName).type(indexDocType(l)).build()
                    );
                }
            }
        }
        return reqs;
    }

    /**
     * Create a JEST delete requests for a specific label
     *
     * @param node  The node to delete in labels' indexes
     * @param label The label that is used to retrieve indices
     * @return The ES REST actions to perform
     */
    public static Map<DocumentIndexId, BulkableAction> deleteRequestsAction(Node node, Label label) {
        HashMap<DocumentIndexId, BulkableAction> reqs = new HashMap<>();

        // we check if it's a managed label
        if (ElasticSearchConfig.indices().containsKey(label.name())) {

            // We check all the indices definition for the label (yes a label can be used in multiple indices)
            for (ElasticSearchIndexSpec spec : ElasticSearchConfig.indices().get(label.name())) {

                String id = id(node);
                String indexName = spec.getIndexName();
                reqs.put(new DocumentIndexId(indexName, id),
                        new Delete.Builder(id)
                                .index(indexName)
                                .type(indexDocType(label))
                                .build());
            }
        }
        return reqs;
    }

    /**
     * Create a JEST index requests.
     *
     * @param node The node to index
     * @return The ES REST actions to perform
     */
    public static Map<DocumentIndexId, BulkableAction> indexRequestsAction(Node node) {
        HashMap<DocumentIndexId, BulkableAction> reqs = new HashMap<>();

        // For each node's label, we check if we have to index it
        for (Label l : node.getLabels()) {
            if (ElasticSearchConfig.indices().containsKey(l.name())) {

                // We check all the indices definition for the label (yes a node can be in multiple indices)
                for (ElasticSearchIndexSpec spec : ElasticSearchConfig.indices().get(l.name())) {

                    if (shouldBeDeleted(node, spec)) {
                        reqs.putAll(deleteRequestsAction(node, l));
                    } else {
                        String id = id(node);
                        String indexName = spec.getIndexName();
                        reqs.put(
                                new DocumentIndexId(indexName, id),
                                new Index.Builder(nodeToJson(node, spec.getProperties()))
                                        .index(indexName)
                                        .id(id)
                                        .type(indexDocType(l))
                                        .build()
                        );
                    }
                }
            }

        }
        return reqs;
    }

    /**
     * Test if node should be deleted or not, corresponding to the index specification
     * A node that has no more indexed field should be removed.
     *
     * @param node
     * @param spec
     * @return <code>true</code> if the node should be delete, otherwise <code>false</code>
     */
    private static Boolean shouldBeDeleted(Node node, ElasticSearchIndexSpec spec) {
        Boolean shouldBeDeleted = Boolean.TRUE;
        for (String field : spec.getProperties()) {
            if (node.getProperty(field, null) != null) {
                shouldBeDeleted = Boolean.FALSE;
            }
        }
        return shouldBeDeleted;
    }

    private static String[] labels(Node node) {
        List<String> result = new ArrayList<>();
        for (Label label : node.getLabels()) {
            result.add(label.name());
        }
        return result.toArray(new String[result.size()]);
    }

    private static String id(Node node) {
        return String.valueOf(node.getId());
    }

    private static String indexDocType(Label label) {
        if (ElasticSearchConfig.configIsEnabled(ElasticSearchConfig.CONFIG_ES_USE_INDEX_TYPE, true)) {
            return label.name();
        }
        return "_doc";
    }

    private static Map nodeToJson(Node node, Set<String> properties) {
        Map<String, Object> json = new LinkedHashMap<>();

        if (ElasticSearchConfig.configIsEnabled(ElasticSearchConfig.CONFIG_ES_INCLUDE_ID, true)) {
            json.put("id", id(node));
        }

        if (ElasticSearchConfig.configIsEnabled(ElasticSearchConfig.CONFIG_ES_INCLUDE_LABELS, true)) {
            json.put("labels", labels(node));
        }

        for (String prop : properties) {
            if (node.hasProperty(prop)) {
                Object value = node.getProperty(prop);

                // Neo4j Point
                if (value instanceof Point) {
                    Point point = (Point) value;
                    value = point.getCoordinate().getCoordinate();
                }
                // Neo4j Date
                if (value instanceof LocalDate) {
                    LocalDate localDate = (LocalDate) value;
                    DateTimeFormatter localDateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
                    value = localDate.format(localDateFormatter);
                }
                // Neo4j Date time
                if (value instanceof ZonedDateTime) {
                    ZonedDateTime zonedDateTime = (ZonedDateTime) value;
                    DateTimeFormatter zoneDateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
                    value = zonedDateTime.format(zoneDateTimeFormatter);
                }
                // Neo4j Local Date time
                if (value instanceof LocalDateTime) {
                    LocalDateTime localDateTime = (LocalDateTime) value;
                    DateTimeFormatter localDateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
                    value = localDateTime.format(localDateTimeFormatter);
                }
                // Neo4j time
                if (value instanceof OffsetTime) {
                    OffsetTime offsetTime = (OffsetTime) value;
                    DateTimeFormatter offsetTimeFormatter = DateTimeFormatter.ofPattern("HHmmss'Z'");
                    value = offsetTime.format(offsetTimeFormatter);
                }
                // Neo4j locale time
                if (value instanceof LocalTime) {
                    LocalTime localTime = (LocalTime) value;
                    DateTimeFormatter localTimeFormatter = DateTimeFormatter.ofPattern("HHmmss'Z'");
                    value = localTime.format(localTimeFormatter);
                }
                // Neo4j duration : there is no duration type in ES
                if (value instanceof TemporalAmount) {
                    TemporalAmount temporalAmount = (TemporalAmount) value;
                    Map<String, Long> duration = new HashMap<>();
                    for (TemporalUnit unit : temporalAmount.getUnits()) {
                        duration.put(unit.toString(), temporalAmount.get(unit));
                    }
                    value = duration;
                }

                json.put(prop, value);
            } else {
                json.put(prop, null);
            }
        }
        return json;
    }

    /**
     * Exec the index work.
     *
     * @param bulk  The list of action to perform
     * @param async Should the work be done in async mode ?
     * @throws IOException if an error occurred during the sync process
     */
    public void index(Bulk bulk, boolean async) throws Exception {
        if (async) {
            client.executeAsync(bulk, this);
        } else {
            BulkResult result = client.execute(bulk);
            if (!result.isSucceeded()) {
                throw new Exception("Fail to perform bulk action : " + result.getJsonString());
            }
        }

    }

    /**
     * Shutdown the ES client, by doing a shutdown of the Jest client
     */
    public void shutdown() {
        if (client != null) {
            try {
                client.close();
            } catch (IOException e) {
                //silent exception when closing
                logger.severe(e.getMessage());
            }
        }
    }

    @Override
    public void completed(JestResult jestResult) {
        if (jestResult.isSucceeded() && jestResult.getErrorMessage() == null) {
            logger.fine("ElasticSearch Update Success");
        } else {
            logger.severe("ElasticSearch Update Failed: " + jestResult.getErrorMessage());
        }
    }

    @Override
    public void failed(Exception e) {
        logger.log(Level.WARNING, "Problem Updating ElasticSearch ", e);
    }

}
