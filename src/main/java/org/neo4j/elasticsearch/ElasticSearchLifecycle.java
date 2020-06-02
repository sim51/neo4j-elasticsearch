package org.neo4j.elasticsearch;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.elasticsearch.config.ElasticSearchIndexSpecParser;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;
import org.neo4j.procedure.impl.GlobalProceduresRegistry;
import org.neo4j.elasticsearch.config.ElasticSearchConfig;
import org.neo4j.elasticsearch.config.ElasticSearchIndexSpec;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

/**
 * @author mh
 * @since 25.04.15
 */
public class ElasticSearchLifecycle extends LifecycleAdapter {

    private final LogService log;
    private final Config config;
    private final GraphDatabaseAPI db;
    private final DatabaseManagementService dbms;
    private final GlobalProceduresRegistry globalProceduresRegistry;

    // logger
    private final Log logger;
    // The index Specification
    private Map<String, List<ElasticSearchIndexSpec>> indices = new HashMap<>();
    // The ES client to index nodes
    private ElasticSearchClient client;
    // The ES Event Handler
    private ElasticSearchEventListener listener;

    /**
     * Create the ElasticSearch Extension.
     *
     * @param log                      Neo4j log service
     * @param config                   Neo4j configuration
     * @param db                       Neo4j graph database API
     * @param dbms                     DB Management service
     * @param globalProceduresRegistry procedurre registery
     */
    public ElasticSearchLifecycle(LogService log, Config config, DatabaseManagementService dbms, GraphDatabaseAPI db, GlobalProceduresRegistry globalProceduresRegistry) {
        logger = log.getUserLog(ElasticSearchLifecycle.class);
        logger.info(String.format("[%s] Creating ElasticSearchLifecycle", db.databaseName()));

        // save paramss
        this.config = config;
        this.log = log;
        this.db = db;
        this.dbms = dbms;
        this.globalProceduresRegistry = globalProceduresRegistry;

        // expose this  instance via `@Context ElasticSearchLifecycle config`
        globalProceduresRegistry.registerComponent((Class<ElasticSearchLifecycle>) getClass(), ctx -> this, true);

    }

    @Override
    public void init() throws Exception {
        if (!SYSTEM_DATABASE_NAME.equals(db.databaseName())) {
            logger.info(String.format("[%s] Starting ElasticSearchLifecycle", db.databaseName()));

            ElasticSearchConfig esConfig = config.getGroups(ElasticSearchConfig.class).get(db.databaseName());

            // parse the index specification
            indices = ElasticSearchIndexSpecParser.parseIndexSpec(config.get(esConfig.CONFIG_ES_INDEX_SPEC));

            // Create the ES client
            client = new ElasticSearchClient(config, db.databaseName());

            // Create the Event Listener
            listener = new ElasticSearchEventListener(client);

            // Register Event Listener
            dbms.registerTransactionEventListener(db.databaseName(), listener);
        }
    }

    @Override
    public void shutdown() {
        if (!SYSTEM_DATABASE_NAME.equals(db.databaseName())) {
            logger.info(String.format("[%s] Stopping ElasticSearch lifecycle", db.databaseName()));

            // Remove the event handler
            if (listener != null) {
                dbms.unregisterTransactionEventListener(db.databaseName(), listener);
            }

            // Shutdown the ES client
            if (client != null) {
                client.shutdown();
            }
        }
    }

}
