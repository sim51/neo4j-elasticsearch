package org.neo4j.elasticsearch;

import io.searchbox.action.BulkableAction;
import io.searchbox.core.Bulk;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.elasticsearch.model.DocumentIndexId;
import org.neo4j.elasticsearch.model.IndexAllResult;
import org.neo4j.graphdb.*;
import org.neo4j.procedure.*;
import org.neo4j.elasticsearch.config.ElasticSearchConfig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class ElasticSearchProcedures {

    private final static Long DEFAULT_BATCH_SIZE = Long.valueOf(500);
    private final static Boolean DEFAULT_ASYNC = Boolean.FALSE;

    @Context
    public GraphDatabaseService db;

    @Context
    public DependencyResolver dependencyResolver;

    @Context
    public Transaction tx;

    private static Long getBatchSize(Map<String, Object> config) {
        return (Long) config.getOrDefault("batchSize", DEFAULT_BATCH_SIZE);
    }

    private static Boolean getAsync(Map<String, Object> config) {
        return (Boolean) config.getOrDefault("async", DEFAULT_ASYNC);
    }

    @Procedure(value = "elasticsearch.index", mode = Mode.SCHEMA)
    @Description("elasticsearch.index(labels, { batchSize:500, async:false }) - Index all the node of the specified labels")
    public Stream<IndexAllResult> index(@Name("label") List<String> labels, @Name("config") Map<String, Object> config) {
        return Stream.of(index(labels, getAsync(config), getBatchSize(config)));
    }

    @Procedure(value = "elasticsearch.indexAll", mode = Mode.SCHEMA)
    @Description("elasticsearch.indexAll({ batchSize:500, async:false }) - Index all the node of the specified labels")
    public Stream<IndexAllResult> indexAll(@Name("config") Map<String, Object> config) {
        List<String> labels = new ArrayList<>();
        for (Label label : tx.getAllLabels()) {
            labels.add(label.name());
        }
        return index(labels, config);
    }

    private IndexAllResult index(List<String> labels, boolean async, long batchSize) {

        // init variables
        Map<DocumentIndexId, BulkableAction> actions = new HashMap<>(1000);
        long nbBatch = 0;
        long nbDoc = 0;

        // Create the ES client
        Config config = dependencyResolver.resolveDependency(Config.class);
        ElasticSearchConfig esConfig = config.getGroups(ElasticSearchConfig.class).get(db.databaseName());

        try (ElasticSearchClient client = new ElasticSearchClient(config, db.databaseName());) {
            // For all labels check if it's an indexed label
            for (String label : labels) {
                if (client.indices.containsKey(label)) {
                    // retrieve all the node with the label
                    try (ResourceIterator<Node> nodes = tx.findNodes(Label.label(label))) {
                        while (nodes.hasNext()) {
                            Node node = nodes.next();
                            nbDoc++;
                            actions.putAll(client.indexRequestsAction(node));

                            if (actions.size() == batchSize || (!nodes.hasNext() && actions.size() > 0)) {
                                Bulk bulk = new Bulk.Builder().addAction(actions.values()).build();
                                client.index(bulk, async);
                                actions.clear();
                                nbBatch++;
                            }
                        }
                    }
                }
            }

            return new IndexAllResult(nbBatch, nbDoc);
        } catch (Exception e) {
            throw new RuntimeException("Failed to re index", e);
        }
    }

}
