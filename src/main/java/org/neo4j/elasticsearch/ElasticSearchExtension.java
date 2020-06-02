package org.neo4j.elasticsearch;

import org.neo4j.elasticsearch.config.ElasticSearchConfig;
import org.neo4j.elasticsearch.model.DocumentIndexId;
import org.neo4j.elasticsearch.model.IndexAllResult;
import io.searchbox.action.BulkableAction;
import io.searchbox.core.Bulk;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * @author mh
 * @since 25.04.15
 */
public class ElasticSearchExtension extends LifecycleAdapter {

    // The logger
    private final static Logger logger = Logger.getLogger(ElasticSearchExtension.class.getName());
    // Instance of the graphdb service
    private final GraphDatabaseService gds;

    // The ES client to index nodes
    private ElasticSearchClient client;
    // The ES Event Handler
    private ElasticSearchEventHandler handler;
    // Is the extension enabled ?
    private boolean enabled = false;

    /**
     * Create the ElasticSearch Extension.
     *
     * @param gds The Neo4j graph database service
     */
    public ElasticSearchExtension(GraphDatabaseService gds) {
        this.gds = gds;
        if (ElasticSearchConfig.indices().size() > 0) {
            enabled = true;
        }
    }

    /**
     * Execute a full re-index job for the specified labels.
     *
     * @param labels    The list of lables to reindex
     * @param async     Should the work must be done in async mode ?
     * @param batchSize The size of batch sent to Elastic
     * @return The result of the process
     * @throws IOException if an error occurred during the ES load
     */
    public IndexAllResult reIndex(List<String> labels, boolean async, long batchSize) throws Exception {
        long nbBatch = 0;
        long nbDoc = 0;
        Map<DocumentIndexId, BulkableAction> actions = new HashMap<>(1000);

        // For all labels check if it's an indexed label
        for (String label : labels) {
            if (ElasticSearchConfig.indices().containsKey(label)) {
                // retrieve all the node with the label
                try (ResourceIterator<Node> nodes = gds.findNodes(Label.label(label))) {
                    while (nodes.hasNext()) {
                        Node node = nodes.next();
                        nbDoc++;
                        actions.putAll(ElasticSearchClient.indexRequestsAction(node));

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
    }

    @Override
    public void init() throws Throwable {
        if (enabled) {
            client = new ElasticSearchClient();

            // Create the Event handler
            handler = new ElasticSearchEventHandler(client);
            gds.registerTransactionEventHandler(handler);

            logger.info("Connecting to ElasticSearch");
        }
    }

    @Override
    public void shutdown() {
        if (!enabled) return;

        // Remove the event handler
        gds.unregisterTransactionEventHandler(handler);

        // Shutdown the ES client
        client.shutdown();

        logger.info("Disconnected from ElasticSearch");
    }

}
