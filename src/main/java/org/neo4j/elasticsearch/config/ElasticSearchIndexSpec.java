package org.neo4j.elasticsearch.config;

import java.util.LinkedHashSet;
import java.util.Set;

public class ElasticSearchIndexSpec {

    private final String indexName;
    private Set<String> properties = new LinkedHashSet<String>();

    public ElasticSearchIndexSpec(String indexName, Set<String> properties) {
        this.indexName = indexName;
        this.properties = properties;
    }

    public String getIndexName() {
        return indexName;
    }

    public Set<String> getProperties() {
        return properties;
    }

    @Override
    public String toString() {
        String s = getClass().getSimpleName() + " " + indexName + ": (";
        for (String p : properties) {
            s += p + ",";
        }
        s += ")";
        return s;
    }
}
