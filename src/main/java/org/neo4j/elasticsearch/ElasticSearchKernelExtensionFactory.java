package org.neo4j.elasticsearch;

import org.neo4j.elasticsearch.config.ElasticSearchConfig;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.extension.ExtensionType;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.Lifecycle;

/**
 * @author mh
 * @since 06.02.13
 */
public class ElasticSearchKernelExtensionFactory extends KernelExtensionFactory<ElasticSearchKernelExtensionFactory.Dependencies> {

    public static final String SERVICE_NAME = "ELASTIC_SEARCH";

    public ElasticSearchKernelExtensionFactory() {
        super(ExtensionType.DATABASE, SERVICE_NAME);
    }

    @Override
    public Lifecycle newInstance(KernelContext kernelContext, Dependencies dependencies) {
        // Initialize the configuration
        ElasticSearchConfig.initialize(dependencies.graphdatabaseAPI());
        // Create the extension
        return new ElasticSearchExtension(dependencies.getGraphDatabaseService());
    }

    public interface Dependencies {
        GraphDatabaseAPI graphdatabaseAPI();

        GraphDatabaseService getGraphDatabaseService();
    }
}
