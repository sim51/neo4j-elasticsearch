package org.neo4j.elasticsearch.model;

public class IndexAllResult {
    public final long numberOfBatches;
    public final long numberOfIndexedDocument;

    public IndexAllResult(long numberOfBatches, long numberOfIndexedDocument) {
        this.numberOfBatches = numberOfBatches;
        this.numberOfIndexedDocument = numberOfIndexedDocument;
    }
}
