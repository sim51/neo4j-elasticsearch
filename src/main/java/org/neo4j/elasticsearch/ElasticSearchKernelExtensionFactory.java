package org.neo4j.elasticsearch;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.ExtensionType;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.internal.LogService;
import org.neo4j.procedure.impl.GlobalProceduresRegistry;

/**
 * @author mh
 * @since 06.02.13
 */
public class ElasticSearchKernelExtensionFactory extends ExtensionFactory<ElasticSearchKernelExtensionFactory.Dependencies> {

    public static final String SERVICE_NAME = "ELASTIC_SEARCH";

    public ElasticSearchKernelExtensionFactory() {
        super(ExtensionType.DATABASE, SERVICE_NAME);
    }

    @Override
    public Lifecycle newInstance(ExtensionContext context, Dependencies dependencies) {
        // Create the extension
        GraphDatabaseAPI db = dependencies.graphdatabaseAPI();
        LogService log = dependencies.log();
        DatabaseManagementService dbms = dependencies.databaseManagementService();
        GlobalProceduresRegistry globalProceduresRegistry = dependencies.globalProceduresRegistry();
        Config config = dependencies.config();
        return new ElasticSearchLifecycle(log, config, dbms, db, globalProceduresRegistry);
    }

    public interface Dependencies {
        Config config();

        DatabaseManagementService databaseManagementService();

        GlobalProceduresRegistry globalProceduresRegistry();

        GraphDatabaseAPI graphdatabaseAPI();

        LogService log();

    }
}
