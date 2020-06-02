package org.neo4j.elasticsearch;

import org.neo4j.elasticsearch.config.ElasticSearchConfig;
import org.neo4j.elasticsearch.model.DocumentIndexId;
import io.searchbox.action.BulkableAction;
import io.searchbox.core.Bulk;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.event.LabelEntry;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventHandler;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author mh
 * @since 25.04.15
 */
class ElasticSearchEventHandler implements TransactionEventHandler<Collection<BulkableAction>> {
    private final static Logger logger = Logger.getLogger(ElasticSearchEventHandler.class.getName());
    private final ElasticSearchClient client;
    private final Set<String> indexLabels;

    public ElasticSearchEventHandler(ElasticSearchClient client) {
        this.client = client;
        indexLabels = ElasticSearchConfig.indices().keySet();
    }

    @Override
    public Collection<BulkableAction> beforeCommit(TransactionData transactionData) throws Exception {
        Map<DocumentIndexId, BulkableAction> actions = new HashMap<>(1000);

        // Check the created node
        for (Node node : transactionData.createdNodes()) {
            if (hasIndexLabel(node)) {
                actions.putAll(ElasticSearchClient.indexRequestsAction(node));
            }
        }

        // Check for deleted node
        // Because we can't know theirs labels, we execute one delete request per index.
        for (Node node : transactionData.deletedNodes()) {
            for (String l : indexLabels) {
                actions.putAll(ElasticSearchClient.deleteRequestsAction(node, Label.label(l)));
            }
        }

        // Check for added labels
        for (LabelEntry labelEntry : transactionData.assignedLabels()) {
            if (hasIndexLabel(labelEntry)) {
                if (transactionData.isDeleted(labelEntry.node())) {
                    actions.putAll(client.deleteRequestsAction(labelEntry.node()));
                } else {
                    actions.putAll(ElasticSearchClient.indexRequestsAction(labelEntry.node()));
                }
            }
        }

        // Check for removed labels
        for (LabelEntry labelEntry : transactionData.removedLabels()) {
            if (hasIndexLabel(labelEntry))
                actions.putAll(client.deleteRequestsAction(labelEntry.node(), labelEntry.label()));
        }

        // Check for properties
        for (PropertyEntry<Node> propEntry : transactionData.assignedNodeProperties()) {
            if (hasIndexLabel(propEntry))
                actions.putAll(ElasticSearchClient.indexRequestsAction(propEntry.entity()));
        }

        // Check for removed properties
        for (PropertyEntry<Node> propEntry : transactionData.removedNodeProperties()) {
            if (!transactionData.isDeleted(propEntry.entity()) && hasIndexLabel(propEntry))
                actions.putAll(ElasticSearchClient.indexRequestsAction(propEntry.entity()));
        }

        return actions.isEmpty() ? Collections.<BulkableAction>emptyList() : actions.values();
    }

    @Override
    public void afterCommit(TransactionData transactionData, Collection<BulkableAction> actions) {
        if (!actions.isEmpty()) {
            Bulk bulk = new Bulk.Builder().addAction(actions).build();
            try {
                client.index(bulk, ElasticSearchConfig.configIsEnabled(ElasticSearchConfig.CONFIG_ES_ASYNC, true));
            } catch (Exception e) {
                logger.log(Level.WARNING, "Error updating ElasticSearch ", e);
            }
        }
    }

    private boolean hasIndexLabel(Node node) {
        for (Label l : node.getLabels()) {
            if (indexLabels.contains(l.name())) {
                return true;
            }
        }
        return false;
    }

    private boolean hasIndexLabel(LabelEntry labelEntry) {
        return indexLabels.contains(labelEntry.label().name());
    }

    private boolean hasIndexLabel(PropertyEntry<Node> propEntry) {
        return hasIndexLabel(propEntry.entity());
    }

    @Override
    public void afterRollback(TransactionData transactionData, Collection<BulkableAction> actions) {
    }

}
