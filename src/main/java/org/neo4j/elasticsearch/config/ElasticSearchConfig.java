package org.neo4j.elasticsearch.config;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.text.ParseException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class ElasticSearchConfig {

    // Prefix for the configuration keys
    public static final String PREFIX = "elasticsearch.";
    // List of configurations
    public static final String CONFIG_ES_URL = PREFIX + "host_name";
    public static final String CONFIG_ES_INDEX_SPEC = PREFIX + "index_spec";
    public static final String CONFIG_ES_DISCOVERY = PREFIX + "discovery";
    public static final String CONFIG_ES_INCLUDE_ID = PREFIX + "include_id_field";
    public static final String CONFIG_ES_INCLUDE_LABELS = PREFIX + "include_labels_field";
    public static final String CONFIG_ES_USE_INDEX_TYPE = PREFIX + "type_mapping";
    public static final String CONFIG_ES_ASYNC = PREFIX + "async";
    public static final String CONFIG_ES_CONN_TIMEOUT = PREFIX + "connection_timeout";
    public static final String CONFIG_ES_READ_TIMEOUT = PREFIX + "read_timeout";
    public static final String CONFIG_ES_USER = PREFIX + "user";
    public static final String CONFIG_ES_PASSWORD = PREFIX + "password";
    // Logger
    private final static Logger logger = Logger.getLogger(ElasticSearchConfig.class.getName());
    // Extension's configuration
    private static final Map<String, Object> config = new HashMap<>(10);
    // Extension indices
    private static Map<String, List<ElasticSearchIndexSpec>> indices = new HashMap<>(10);

    /**
     * Retrieve the configuration for the specified key, or give the default value.
     *
     * @param key          The config key to retrieve
     * @param defaultValue The default value if the key is not found
     * @return The string value of the configuration key
     */
    public static <T> T getConfig(String key, T defaultValue) {
        return (T) config.getOrDefault(key, defaultValue);
    }

    /**
     * Read and initialize the extension configuration.
     *
     * @param db The Neo4j graphdb API service
     */
    public static void initialize(GraphDatabaseAPI db) {
        // Retrieve the all neo4j configuration
        Config neo4jConfig = db.getDependencyResolver().resolveDependency(Config.class);
        Map<String, String> params = neo4jConfig.getRaw();

        // Filter the neo4j configuration with the prefix
        config.clear();
        config.putAll(
                params.entrySet()
                        .stream()
                        .filter(map -> map.getKey().startsWith(PREFIX))
                        .collect(Collectors.toMap(map -> map.getKey(), map -> map.getValue()))
        );

        // Parsing and saving the indices configuration
        indices.clear();
        try {
            indices = ElasticSearchIndexSpecParser.parseIndexSpec(ElasticSearchConfig.getConfig(ElasticSearchConfig.CONFIG_ES_INDEX_SPEC, ""));
        } catch (ParseException e) {
            logger.severe("Elastic integration - Bad configuration.Check that you haven't defined an index twice");
        }
        if (indices.size() == 0) {
            logger.warning("Elastic integration - Configuration is missing or empty");
        }
    }

    /**
     * Retrieve the configuration for the specified key, or give the default value.
     *
     * @param key          The config key to retrieve
     * @param defaultValue The default value if the key is not found
     * @return The boolean value of the configuration key
     */
    public static boolean configIsEnabled(String key, boolean defaultValue) {
        String result = getConfig(key, String.valueOf(defaultValue));
        if (result != null) {
            if (result.equals("true")) {
                return true;
            } else {
                return false;
            }
        } else {
            return defaultValue;
        }
    }

    /**
     * Set the configuration for the specified key, or give the default value.
     *
     * @param key   The config key to set
     * @param value The value to set
     */
    public static void setConfig(String key, String value) {
        config.put(key, value);
    }

    /**
     * Get the entire extension configuration.
     *
     * @return All configurations that are linked to this plugin.
     */
    public static Map<String, Object> config() {
        return config;
    }


    /**
     * Get the entire indices configuration per label.
     *
     * @return The index spec per label
     */
    public static Map<String, List<ElasticSearchIndexSpec>> indices() {
        return indices;
    }
}
