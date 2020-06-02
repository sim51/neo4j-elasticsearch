package org.neo4j.elasticsearch.model;

/**
 * Model to create a unique id for a document in an index.
 */
public class DocumentIndexId {

    // name of the index
    private final String indexName;
    // Id of the document
    private final String id;

    public DocumentIndexId(String indexName, String id) {
        this.indexName = indexName;
        this.id = id;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + getClass().hashCode();
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        result = prime * result + ((indexName == null) ? 0 : indexName.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (!(obj instanceof DocumentIndexId))
            return false;
        DocumentIndexId other = (DocumentIndexId) obj;
        if (id == null) {
            if (other.id != null)
                return false;
        } else if (!id.equals(other.id))
            return false;
        if (indexName == null) {
            if (other.indexName != null)
                return false;
        } else if (!indexName.equals(other.indexName))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "IndexId [indexName=" + indexName + ", id=" + id + "]";
    }
}
