package org.neo4j.elasticsearch.config;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Description;
import org.neo4j.configuration.DocumentedDefaultValue;
import org.neo4j.configuration.GroupSetting;
import org.neo4j.graphdb.config.Setting;

import static org.neo4j.configuration.SettingValueParsers.*;

@ServiceProvider
public class ElasticSearchConfig extends GroupSetting {

    // Prefix for the configuration keys
    public final String PREFIX = "elasticsearch";

    @Description("ElasticSearch URL")
    @DocumentedDefaultValue("http://localhost:9200")
    public final Setting<String> CONFIG_ES_URL = getBuilder("host_name", STRING, "http://localhost:9200").build();

    @Description("ElasticSearch index specification")
    public final Setting<String> CONFIG_ES_INDEX_SPEC = getBuilder("index_spec", STRING, "").build();

    @Description("Discover other ElasticSearch cluster node ?")
    @DocumentedDefaultValue("false")
    public final Setting<Boolean> CONFIG_ES_DISCOVERY = getBuilder("discovery", BOOL, Boolean.FALSE).build();

    @Description("Should ElasticSearch indexation includes node's ID ?")
    @DocumentedDefaultValue("true")
    public final Setting<Boolean> CONFIG_ES_INCLUDE_ID = getBuilder("include_id_field", BOOL, Boolean.TRUE).build();

    @Description("Should ElasticSearch indexation includes node's Labels ?")
    @DocumentedDefaultValue("true")
    public final Setting<Boolean> CONFIG_ES_INCLUDE_LABELS = getBuilder("include_labels_field", BOOL, Boolean.TRUE).build();

    @Description("Should ElasticSearch indexation includes database name ?")
    @DocumentedDefaultValue("true")
    public final Setting<Boolean> CONFIG_ES_INCLUDE_DB = getBuilder("include_db_field", BOOL, Boolean.FALSE).build();

    @Description("Should ElasticSearch indexation use labels as types ?")
    @DocumentedDefaultValue("false")
    public final Setting<Boolean> CONFIG_ES_USE_INDEX_TYPE = getBuilder("type_mapping", BOOL, Boolean.FALSE).build();

    @Description("Should ElasticSearch indexation use async ?")
    @DocumentedDefaultValue("true")
    public final Setting<Boolean> CONFIG_ES_ASYNC = getBuilder("async", BOOL, Boolean.TRUE).build();

    @Description("ElasticSearch user's name")
    public final Setting<String> CONFIG_ES_USER = getBuilder("user", STRING, null).build();

    @Description("ElasticSearch user's password")
    public final Setting<String> CONFIG_ES_PASSWORD = getBuilder("password", STRING, null).build();

    @Description("ElasticSearch connection timeout (ms)")
    public final Setting<Integer> CONFIG_ES_CONN_TIMEOUT = getBuilder("connection_timeout", INT, null).build();

    @Description("ElasticSearch read timeout in (ms)")
    public final Setting<Integer> CONFIG_ES_READ_TIMEOUT = getBuilder("read_timeout", INT, null).build();

    private ElasticSearchConfig(String dbname) {
        super(dbname);
        if (dbname == null) {
            throw new IllegalArgumentException("ElasticSearchConfig can not be created for scope: " + dbname);
        }
    }

    //For serviceloading
    public ElasticSearchConfig() {
        this("testing");
    }

    public static ElasticSearchConfig forDatabase(String dbname) {
        return new ElasticSearchConfig(dbname);
    }

    @Override
    public String getPrefix() {
        return PREFIX;
    }
}
