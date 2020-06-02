package org.neo4j.elasticsearch;

import io.searchbox.action.BulkableAction;
import io.searchbox.core.Bulk;
import org.neo4j.elasticsearch.model.DocumentIndexId;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.LabelEntry;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventListener;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author mh
 * @since 25.04.15
 */
class ElasticSearchEventListener implements TransactionEventListener<Collection<BulkableAction>> {

    private final static Logger logger = Logger.getLogger(ElasticSearchEventListener.class.getName());
    private final ElasticSearchClient client;

    public ElasticSearchEventListener(ElasticSearchClient client) {
        this.client = client;
    }

    @Override
    public Collection<BulkableAction> beforeCommit(TransactionData data, Transaction transaction, GraphDatabaseService databaseService) {
        Map<DocumentIndexId, BulkableAction> actions = new HashMap<>(1000);

        // Check the created node
        for (Node node : data.createdNodes()) {
            if (hasIndexLabel(node)) {
                actions.putAll(client.indexRequestsAction(node));
            }
        }

        // Check for deleted node
        // Because we can't know theirs labels, we execute one delete request per index.
        for (Node node : data.deletedNodes()) {
            for (String l : client.indices.keySet()) {
                actions.putAll(client.deleteRequestsAction(node, Label.label(l)));
            }
        }

        // Check for added labels
        for (LabelEntry labelEntry : data.assignedLabels()) {
            if (hasIndexLabel(labelEntry)) {
                if (data.isDeleted(labelEntry.node())) {
                    actions.putAll(client.deleteRequestsAction(labelEntry.node()));
                } else {
                    actions.putAll(client.indexRequestsAction(labelEntry.node()));
                }
            }
        }

        // Check for removed labels
        for (LabelEntry labelEntry : data.removedLabels()) {
            if (hasIndexLabel(labelEntry))
                actions.putAll(client.deleteRequestsAction(labelEntry.node(), labelEntry.label()));
        }

        // Check for properties
        for (PropertyEntry<Node> propEntry : data.assignedNodeProperties()) {
            if (hasIndexLabel(propEntry))
                actions.putAll(client.indexRequestsAction(propEntry.entity()));
        }

        // Check for removed properties
        for (PropertyEntry<Node> propEntry : data.removedNodeProperties()) {
            if (!data.isDeleted(propEntry.entity()) && hasIndexLabel(propEntry))
                actions.putAll(client.indexRequestsAction(propEntry.entity()));
        }

        logger.finest(String.format("[%s] Before commit : Found %d action to perform on ElasticSearch", databaseService.databaseName(), actions.size()));
        return actions.isEmpty() ? Collections.<BulkableAction>emptyList() : actions.values();
    }

    @Override
    public void afterCommit(TransactionData data, Collection<BulkableAction> actions, GraphDatabaseService databaseService) {
        if (!actions.isEmpty()) {
            Bulk bulk = new Bulk.Builder().addAction(actions).build();
            try {
                client.index(bulk);
            } catch (Exception e) {
                logger.log(Level.SEVERE, String.format("[%s] Error updating ElasticSearch", databaseService.databaseName()), e);
            }
        }
    }

    @Override
    public void afterRollback(TransactionData data, Collection<BulkableAction> state, GraphDatabaseService databaseService) {

    }

    private boolean hasIndexLabel(Node node) {
        for (Label l : node.getLabels()) {
            if (client.indices.containsKey(l.name())) {
                return true;
            }
        }
        return false;
    }

    private boolean hasIndexLabel(LabelEntry labelEntry) {
        return client.indices.containsKey(labelEntry.label().name());
    }

    private boolean hasIndexLabel(PropertyEntry<Node> propEntry) {
        return hasIndexLabel(propEntry.entity());
    }


}
