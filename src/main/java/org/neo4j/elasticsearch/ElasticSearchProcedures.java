package org.neo4j.elasticsearch;

import org.neo4j.elasticsearch.model.IndexAllResult;
import org.neo4j.graphdb.Label;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class ElasticSearchProcedures {

    private final static Long DEFAULT_BATCH_SIZE = Long.valueOf(500);
    private final static Boolean DEFAULT_ASYNC = Boolean.FALSE;

    @Context
    public GraphDatabaseAPI db;

    private static Long getBatchSize(Map<String, Object> config) {
        return (Long) config.getOrDefault("batchSize", DEFAULT_BATCH_SIZE.toString());
    }

    private static Boolean getAsync(Map<String, Object> config) {
        return (Boolean) config.getOrDefault("async", DEFAULT_ASYNC);
    }

    @Procedure(value = "elasticsearch.index", mode = Mode.SCHEMA)
    @Description("elasticsearch.index(labels, { batchSize:500, async:false }) - Index all the node of the specified labels")
    public Stream<IndexAllResult> index(@Name("label") List<String> labels, @Name("config") Map<String, Object> config) {
        try {
            ElasticSearchExtension esExtension = db.getDependencyResolver().resolveDependency(ElasticSearchExtension.class);
            if (esExtension != null) {

                return Stream.of(esExtension.reIndex(labels, getAsync(config), getBatchSize(config)));
            } else {
                throw new RuntimeException("Can't find elastic extension");
            }
        } catch (Exception e) {
            throw new RuntimeException("failed to reset index update configuration", e);
        }
    }

    @Procedure(value = "elasticsearch.indexAll", mode = Mode.SCHEMA)
    @Description("elasticsearch.indexAll({ batchSize:500, async:false }) - Index all the node of the specified labels")
    public Stream<IndexAllResult> indexAll(@Name("config") Map<String, Object> config) {
        List<String> labels = new ArrayList<>();
        for (Label label : db.getAllLabelsInUse()) {
            labels.add(label.name());
        }
        return index(labels, config);
    }

}
