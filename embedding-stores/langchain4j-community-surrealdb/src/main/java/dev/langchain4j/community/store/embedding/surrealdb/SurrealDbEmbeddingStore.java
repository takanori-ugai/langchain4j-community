package dev.langchain4j.community.store.embedding.surrealdb;

import com.surrealdb.Surreal;
import com.surrealdb.signin.Root;
import com.surrealdb.Response;
import com.surrealdb.Value;
import com.surrealdb.Array;
import com.surrealdb.Object;
import com.surrealdb.RecordId;
import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.store.embedding.EmbeddingMatch;
import dev.langchain4j.store.embedding.EmbeddingSearchRequest;
import dev.langchain4j.store.embedding.EmbeddingSearchResult;
import dev.langchain4j.store.embedding.EmbeddingStore;
import dev.langchain4j.store.embedding.filter.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static dev.langchain4j.internal.Utils.randomUUID;
import static dev.langchain4j.internal.ValidationUtils.ensureNotNull;

/**
 * Represents a <a href="https://surrealdb.com/">SurrealDB</a> index as an embedding store.
 */
public class SurrealDbEmbeddingStore implements EmbeddingStore<TextSegment> {

    private static final Logger log = LoggerFactory.getLogger(SurrealDbEmbeddingStore.class);

    private final Surreal driver;
    private final String namespace;
    private final String database;
    private final String collection;
    private final int dimension;

    public SurrealDbEmbeddingStore(Surreal driver, String namespace, String database, String collection, int dimension) {
        this.driver = ensureNotNull(driver, "driver");
        this.namespace = ensureNotNull(namespace, "namespace");
        this.database = ensureNotNull(database, "database");
        this.collection = ensureNotNull(collection, "collection");
        this.dimension = dimension;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public String add(Embedding embedding) {
        String id = randomUUID();
        add(id, embedding);
        return id;
    }

    @Override
    public void add(String id, Embedding embedding) {
        addInternal(id, embedding, null);
    }

    @Override
    public String add(Embedding embedding, TextSegment textSegment) {
        String id = randomUUID();
        addInternal(id, embedding, textSegment);
        return id;
    }

    @Override
    public List<String> addAll(List<Embedding> embeddings) {
        return addAll(embeddings, null);
    }

    @Override
    public List<String> addAll(List<Embedding> embeddings, List<TextSegment> embedded) {
        if (embeddings == null || embeddings.isEmpty()) {
            return new ArrayList<>();
        }

        List<String> ids = new ArrayList<>();
        List<SurrealEmbeddingRecord> records = new ArrayList<>();

        for (int i = 0; i < embeddings.size(); i++) {
            String id = randomUUID();
            ids.add(id);
            TextSegment segment = embedded != null && i < embedded.size() ? embedded.get(i) : null;
            records.add(new SurrealEmbeddingRecord(collection, id, embeddings.get(i).vectorAsList(), segment));
        }

        try {
             // Bulk insert not directly supported by create method for different IDs in one go easily without query
             // but we can loop.
             for (SurrealEmbeddingRecord record : records) {
                 driver.create(new RecordId(collection, record.id), record);
             }
        } catch (Exception e) {
            throw new RuntimeException("Failed to add embeddings", e);
        }

        return ids;
    }

    @Override
    public void removeAll(java.util.Collection<String> ids) {
        if (ids == null || ids.isEmpty()) {
            return;
        }
        for (String id : ids) {
            driver.delete(new RecordId(collection, id));
        }
    }

    @Override
    public void removeAll() {
        driver.delete(collection);
    }

    @Override
    public void removeAll(Filter filter) {
        Map<String, java.lang.Object> params = new HashMap<>();
        SurrealDbFilterMapper filterMapper = new SurrealDbFilterMapper(params);
        String whereClause = filterMapper.map(filter);
        String query = "DELETE " + collection + " WHERE " + whereClause;
        driver.queryBind(query, params);
    }

    private void addInternal(String id, Embedding embedding, TextSegment textSegment) {
        SurrealEmbeddingRecord record = new SurrealEmbeddingRecord(collection, id, embedding.vectorAsList(), textSegment);
        driver.create(new RecordId(collection, id), record);
    }

    @Override
    public EmbeddingSearchResult<TextSegment> search(EmbeddingSearchRequest request) {
        String query = "SELECT *, vector::similarity::cosine(embedding, $query_embedding) AS score FROM " + collection;
        Map<String, java.lang.Object> params = new HashMap<>();
        params.put("query_embedding", request.queryEmbedding().vectorAsList());
        params.put("limit", request.maxResults());

        StringBuilder whereClause = new StringBuilder();

        // Add filter
        if (request.filter() != null) {
            SurrealDbFilterMapper filterMapper = new SurrealDbFilterMapper(params);
            whereClause.append(filterMapper.map(request.filter()));
        }

        // Add KNN search
        if (whereClause.length() > 0) {
            whereClause.append(" AND ");
        }

        whereClause.append("embedding <|" + request.maxResults() + "|> $query_embedding");

        query += " WHERE " + whereClause.toString() + " ORDER BY score DESC LIMIT $limit";

        try {
            Response response = driver.queryBind(query, params);

            // Check for errors? Response wrapper might hold errors.
            // Assuming take(0) returns the result of the first query.
            Value result = response.take(0);

            if (result.isNone() || result.isNull()) {
                 return new EmbeddingSearchResult<>(new ArrayList<>());
            }

            List<EmbeddingMatch<TextSegment>> matches = new ArrayList<>();

            if (result.isArray()) {
                Array array = result.getArray();
                for (Value row : array) {
                    if (row.isObject()) {
                        Object obj = row.getObject();

                        // Parse id
                        String id = null;
                        if (obj.get("id").isThing()) {
                            id = obj.get("id").getThing().getId().getString(); // getId() returns Id object, getString() on Id returns string id
                        } else if (obj.get("id").isString()) {
                            id = obj.get("id").getString();
                        }

                        // Parse embedding
                        List<Float> embeddingList = new ArrayList<>();
                        if (obj.get("embedding").isArray()) {
                            for (Value v : obj.get("embedding").getArray()) {
                                if (v.isDouble()) {
                                    embeddingList.add((float)v.getDouble());
                                } else if (v.isString()) {
                                    embeddingList.add(Float.parseFloat(v.getString()));
                                }
                            }
                        }

                        // Parse text and metadata
                        String text = null;
                        if (obj.get("text").isString()) {
                            text = obj.get("text").getString();
                        }

                        Map<String, java.lang.Object> metadataMap = new HashMap<>();
                        if (obj.get("metadata").isObject()) {
                            Object metaObj = obj.get("metadata").getObject();
                            // We need to iterate over keys of metaObj
                            // com.surrealdb.Object implements Iterable<Entry>
                            for (com.surrealdb.Entry entry : metaObj) {
                                String key = entry.getKey();
                                Value val = entry.getValue();
                                if (val.isString()) {
                                    metadataMap.put(key, val.getString());
                                } else if (val.isBoolean()) {
                                    metadataMap.put(key, val.getBoolean());
                                } else if (val.isDouble()) {
                                    metadataMap.put(key, val.getDouble());
                                } else if (val.isLong()) {
                                    metadataMap.put(key, val.getLong());
                                }
                                // Add other types as needed
                            }
                        }

                        // Parse score
                        double score = 0;
                        if (obj.get("score").isDouble()) {
                            score = obj.get("score").getDouble();
                        }

                        if (score >= request.minScore()) {
                            matches.add(new EmbeddingMatch<>(
                                    score,
                                    id,
                                    Embedding.from(embeddingList),
                                    text != null ? TextSegment.from(text, dev.langchain4j.data.document.Metadata.from(metadataMap)) : null
                            ));
                        }
                    }
                }
            }

            return new EmbeddingSearchResult<>(matches);

        } catch (Exception e) {
            throw new RuntimeException("Search failed", e);
        }
    }

    public static class Builder {
        private String host;
        private int port;
        private boolean useTls;
        private String namespace;
        private String database;
        private String username;
        private String password;
        private String collection = "vectors";
        private int dimension;

        public Builder host(String host) {
            this.host = host;
            return this;
        }

        public Builder port(int port) {
            this.port = port;
            return this;
        }

        public Builder useTls(boolean useTls) {
            this.useTls = useTls;
            return this;
        }

        public Builder namespace(String namespace) {
            this.namespace = namespace;
            return this;
        }

        public Builder database(String database) {
            this.database = database;
            return this;
        }

        public Builder username(String username) {
            this.username = username;
            return this;
        }

        public Builder password(String password) {
            this.password = password;
            return this;
        }

        public Builder collection(String collection) {
            this.collection = collection;
            return this;
        }

        public Builder dimension(int dimension) {
            this.dimension = dimension;
            return this;
        }

        public SurrealDbEmbeddingStore build() {
            ensureNotNull(host, "host");
            ensureNotNull(namespace, "namespace");
            ensureNotNull(database, "database");
            ensureNotNull(username, "username");
            ensureNotNull(password, "password");

            Surreal driver = new Surreal();
            // driver.connect accepts String. Protocols: ws://, wss://, http://, https://, memory://
            String protocol = useTls ? "wss" : "ws";
            String connectString = String.format("%s://%s:%d/rpc", protocol, host, port);

            driver.connect(connectString);
            driver.useNs(namespace);
            driver.useDb(database);
            driver.signin(new Root(username, password));

            // Create index if needed
            try {
                 String indexQuery = String.format(
                     "DEFINE INDEX idx_embedding_%s ON TABLE %s FIELDS embedding HNSW DIMENSION %d DIST COSINE TYPE F32",
                     collection, collection, dimension
                 );
                 driver.query(indexQuery);
            } catch (Exception e) {
                log.warn("Failed to create index", e);
            }

            return new SurrealDbEmbeddingStore(driver, namespace, database, collection, dimension);
        }
    }

    // Helper class for serialization
    private static class SurrealEmbeddingRecord {
        // Transient fields to avoid serialization if needed, but here we want them
        transient String collection;
        transient String id;

        List<Float> embedding;
        String text;
        Map<String, java.lang.Object> metadata;

        public SurrealEmbeddingRecord(String collection, String id, List<Float> embedding, TextSegment textSegment) {
            this.collection = collection;
            this.id = id;
            this.embedding = embedding;
            if (textSegment != null) {
                this.text = textSegment.text();
                this.metadata = textSegment.metadata().toMap();
            }
        }
    }
}
