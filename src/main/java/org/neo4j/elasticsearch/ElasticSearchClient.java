package org.neo4j.elasticsearch;

import io.searchbox.action.BulkableAction;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.JestResultHandler;
import io.searchbox.core.Bulk;
import io.searchbox.core.BulkResult;
import io.searchbox.core.Delete;
import io.searchbox.core.Index;
import org.neo4j.configuration.Config;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.elasticsearch.config.ElasticSearchConfig;
import org.neo4j.elasticsearch.config.ElasticSearchIndexSpec;
import org.neo4j.elasticsearch.config.ElasticSearchIndexSpecParser;
import org.neo4j.elasticsearch.model.DocumentIndexId;

import java.io.IOException;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAmount;
import java.time.temporal.TemporalUnit;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ElasticSearchClient implements JestResultHandler<JestResult>, AutoCloseable {

    // Logger
    private final static Logger logger = Logger.getLogger(ElasticSearchClient.class.getName());
    public final Map<String, List<ElasticSearchIndexSpec>> indices;
    // JEST Client
    private final JestClient client;
    private final Boolean useType;
    private final Boolean includeId;
    private final Boolean includeLabels;
    private final Boolean includeDb;
    private final Boolean async;
    private final String dbname;

    /**
     * Initialize the ES client, by creating the Jest client.
     *
     * @param config The neo4j configuration object
     * @param dbname Name of the database
     * @throws Exception in case of a spec parsing or es client init error
     */
    public ElasticSearchClient(Config config, String dbname) throws Exception {
        ElasticSearchConfig esConfig = config.getGroups(ElasticSearchConfig.class).get(dbname);
        this.dbname = dbname;
        indices = ElasticSearchIndexSpecParser.parseIndexSpec(config.get(esConfig.CONFIG_ES_INDEX_SPEC));
        useType = config.get(esConfig.CONFIG_ES_USE_INDEX_TYPE);
        includeId = config.get(esConfig.CONFIG_ES_INCLUDE_ID);
        includeLabels = config.get(esConfig.CONFIG_ES_INCLUDE_LABELS);
        includeDb = config.get(esConfig.CONFIG_ES_INCLUDE_DB);
        async = config.get(esConfig.CONFIG_ES_ASYNC);

        logger.info(String.format(
                "[%s] Creating ElasticSearch client for %s with spec %s",
                dbname,
                config.get(esConfig.CONFIG_ES_URL),
                config.get(esConfig.CONFIG_ES_INDEX_SPEC)
        ));

        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(
                JestDefaultHttpConfigFactory.getConfigFor(
                        config.get(esConfig.CONFIG_ES_URL),
                        config.get(esConfig.CONFIG_ES_DISCOVERY),
                        config.get(esConfig.CONFIG_ES_USER),
                        config.get(esConfig.CONFIG_ES_PASSWORD),
                        config.get(esConfig.CONFIG_ES_CONN_TIMEOUT),
                        config.get(esConfig.CONFIG_ES_READ_TIMEOUT)
                )
        );
        client = factory.getObject();
    }

    /**
     * Test if node should be deleted or not, corresponding to the index specification
     * A node that has no more indexed field should be removed.
     *
     * @param node The node we need to analyse
     * @param spec The index specification
     * @return <code>true</code> if the node should be deleted, <code>false</code> otherwise.
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

    /**
     * Return the labels of the node as an array.
     *
     * @param node the node
     * @return the labels of the node as an array.
     */
    private static String[] labels(Node node) {
        List<String> result = new ArrayList<>();
        for (Label label : node.getLabels()) {
            result.add(label.name());
        }
        return result.toArray(new String[result.size()]);
    }

    /**
     * Return the ID of the node.
     *
     * @param node the node
     * @return The ES doc id of the node
     */
    private String id(Node node) {
        return String.format("%s_%s", dbname, String.valueOf(node.getId()));
    }

    /**
     * Create a JEST index requests.
     *
     * @param node the node to process
     * @return The KV of key/action
     */
    public Map<DocumentIndexId, BulkableAction> indexRequestsAction(Node node) {
        logger.finest(String.format("[%s] Index request for node %d", dbname, node.getId()));
        HashMap<DocumentIndexId, BulkableAction> reqs = new HashMap<>();

        // For each node's label, we check if we have to index it
        for (Label l : node.getLabels()) {
            if (indices.containsKey(l.name())) {

                // We check all the indices definition for the label (yes a node can be in multiple indices)
                for (ElasticSearchIndexSpec spec : indices.get(l.name())) {

                    if (shouldBeDeleted(node, spec)) {
                        reqs.putAll(deleteRequestsAction(node, l));
                    } else {
                        String id = id(node);
                        String indexName = spec.getIndexName();
                        reqs.put(
                                new DocumentIndexId(dbname, indexName, id),
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
     * Create a JEST delete requests
     *
     * @param node the node to process
     * @return The KV of key/action
     */
    public Map<DocumentIndexId, Delete> deleteRequestsAction(Node node) {
        logger.finest(String.format("[%s] Delete request for node %d", dbname, node.getId()));
        HashMap<DocumentIndexId, Delete> reqs = new HashMap<>();

        // For each node's label, we check if we have to de-index it
        for (Label l : node.getLabels()) {
            if (indices.containsKey(l.name())) {

                // We check all the indices definition for the label (yes a node can be in multiple indices)
                for (ElasticSearchIndexSpec spec : indices.get(l.name())) {
                    String id = id(node);
                    String indexName = spec.getIndexName();
                    reqs.put(
                            new DocumentIndexId(dbname, indexName, id),
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
     * @param node  the node to process
     * @param label The specif label
     * @return The KV of key/action
     */
    public Map<DocumentIndexId, BulkableAction> deleteRequestsAction(Node node, Label label) {
        logger.finest(String.format("[%s] Delete request for node %d on label %s", dbname, node.getId(), label.name()));
        HashMap<DocumentIndexId, BulkableAction> reqs = new HashMap<>();

        // we check if it's a managed label
        if (indices.containsKey(label.name())) {

            // We check all the indices definition for the label (yes a label can be used in multiple indices)
            for (ElasticSearchIndexSpec spec : indices.get(label.name())) {

                String id = id(node);
                String indexName = spec.getIndexName();
                reqs.put(new DocumentIndexId(dbname, indexName, id),
                        new Delete.Builder(id)
                                .index(indexName)
                                .type(indexDocType(label))
                                .build());
            }
        }
        return reqs;
    }


    /**
     * Exec the index work.
     *
     * @param bulk The list of action to perform
     * @throws Exception if an error occurred during the process
     */
    public void index(Bulk bulk) throws Exception {
        index(bulk, async);
    }

    /**
     * Exec the index work.
     *
     * @param bulk  The list of action to perform
     * @param async Should the work be done in async mode ?
     * @throws Exception if an error occurred during the process
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
                logger.severe(String.format("[%s] Failed to close Elastic client", dbname));
            }
        }
    }

    @Override
    public void completed(JestResult jestResult) {
        if (jestResult.isSucceeded() && jestResult.getErrorMessage() == null) {
            logger.finest(String.format("[%s] ElasticSearch Update Success", dbname));
        } else {
            logger.severe(String.format("[%s] ElasticSearch Update Failed: %s", dbname, jestResult.getErrorMessage()));
        }
    }

    @Override
    public void failed(Exception e) {
        logger.log(Level.SEVERE, "Problem Updating ElasticSearch ", e);
    }

    /**
     * Return the ES index doc type.
     *
     * @param label The label that helps to compute the doc type
     * @return the ES document type
     */
    private String indexDocType(Label label) {
        if (useType) {
            return label.name();
        }
        return "_doc";
    }


    /**
     * Convert a Neo4j node to a valid JSON for ES.
     *
     * @param node       The node to convert
     * @param properties The list of properties we want in the json
     * @return A map that match the JSON value of the node
     */
    private Map nodeToJson(Node node, Set<String> properties) {
        Map<String, Object> json = new LinkedHashMap<>();

        if (includeId) {
            json.put("@id", String.valueOf(node.getId()));
        }

        if (includeLabels) {
            json.put("@labels", labels(node));
        }

        if (includeDb) {
            json.put("@dbname", dbname);
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


    @Override
    public void close() throws Exception {
        shutdown();
    }
}
