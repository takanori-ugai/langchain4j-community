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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static dev.langchain4j.internal.Utils.randomUUID;
import static dev.langchain4j.internal.ValidationUtils.ensureNotNull;

/**
 * Represents a <a href="https://surrealdb.com/">SurrealDB</a> index as an embedding store.
 */
public class SurrealDbEmbeddingStore implements EmbeddingStore<TextSegment>, java.io.Closeable {

    private final Surreal driver;
    private final String namespace;
    private final String database;
    private final String collection;
    private final int dimension;

    /**
     * Create a SurrealDbEmbeddingStore configured to use the given SurrealDB driver, namespace, database, collection, and embedding dimension.
     *
     * The constructor validates that `driver`, `namespace`, `database`, and `collection` are not null and stores the provided `dimension`.
     *
     * @param driver    the SurrealDB driver instance to use for all database operations
     * @param namespace the SurrealDB namespace to operate in
     * @param database  the SurrealDB database to operate in
     * @param collection the collection (table) name where embeddings are stored
     * @param dimension the dimensionality of embeddings stored in this store
     * @throws NullPointerException if `driver`, `namespace`, `database`, or `collection` is null
     */
    public SurrealDbEmbeddingStore(Surreal driver, String namespace, String database, String collection, int dimension) {
        this.driver = ensureNotNull(driver, "driver");
        this.namespace = ensureNotNull(namespace, "namespace");
        this.database = ensureNotNull(database, "database");
        this.collection = ensureNotNull(collection, "collection");
        this.dimension = dimension;
    }

    /**
     * Closes the underlying SurrealDB driver, if present.
     *
     * This operation is a no-op when no driver was configured.
     */
    @Override
    public void close() {
        if (driver != null) {
            driver.close();
        }
    }

    /**
     * Create a new builder for configuring and constructing a SurrealDbEmbeddingStore.
     *
     * @return a new Builder instance for fluently configuring and building a SurrealDbEmbeddingStore
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Generates a unique id, adds the provided embedding to the store, and returns the id.
     *
     * @param embedding the embedding to store
     * @return the generated id of the stored embedding
     */
    @Override
    public String add(Embedding embedding) {
        String id = randomUUID();
        add(id, embedding);
        return id;
    }

    /**
     * Adds an embedding to the store using the provided identifier without attaching any text segment.
     *
     * @param id        the identifier to assign to the stored embedding record
     * @param embedding the embedding vector to store
     */
    @Override
    public void add(String id, Embedding embedding) {
        addInternal(id, embedding, null);
    }

    /**
     * Stores the given embedding with the provided text segment and returns a generated identifier.
     *
     * @param embedding the embedding vector to store
     * @param textSegment optional text and metadata associated with the embedding; may be null
     * @return the generated identifier for the stored embedding
     */
    @Override
    public String add(Embedding embedding, TextSegment textSegment) {
        String id = randomUUID();
        addInternal(id, embedding, textSegment);
        return id;
    }

    /**
     * Adds multiple embeddings to the store and returns the generated identifiers.
     *
     * <p>If the provided list is null or empty, no records are created and an empty list is returned.</p>
     *
     * @param embeddings the embeddings to add; may be null or empty
     * @return a list of identifiers generated for the inserted embeddings, or an empty list if none were added
     */
    @Override
    public List<String> addAll(List<Embedding> embeddings) {
        return addAll(embeddings, null);
    }

    /**
     * Adds multiple embedding records to the SurrealDB collection, optionally associating each with a provided id and text segment.
     *
     * For each embedding, uses the corresponding id from {@code ids} if provided or generates a random UUID when {@code ids} is null.
     * If {@code embedded} is provided, attaches the text segment at the same index when available.
     *
     * @param ids       optional list of ids; when non-null its size must equal {@code embeddings.size()}
     * @param embeddings list of embeddings to insert; no action is taken if this list is null or empty
     * @param embedded  optional list of TextSegment objects to attach to corresponding embeddings
     * @throws IllegalArgumentException if {@code ids} is non-null and its size does not match {@code embeddings.size()}
     * @throws RuntimeException if insertion of any record into SurrealDB fails
     */
    @Override
    public void addAll(List<String> ids, List<Embedding> embeddings, List<TextSegment> embedded) {
        if (embeddings == null || embeddings.isEmpty()) {
            return;
        }
        if (ids != null && ids.size() != embeddings.size()) {
            throw new IllegalArgumentException("ids size must match embeddings size");
        }

        List<SurrealEmbeddingRecord> records = new ArrayList<>();

        for (int i = 0; i < embeddings.size(); i++) {
            String id = ids != null ? ids.get(i) : randomUUID();
            TextSegment segment = embedded != null && i < embedded.size() ? embedded.get(i) : null;
            records.add(new SurrealEmbeddingRecord(collection, id, embeddings.get(i).vectorAsList(), segment));
        }

        try {
            for (SurrealEmbeddingRecord record : records) {
                driver.create(new RecordId(collection, record.id), record);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to add embeddings", e);
        }
    }

    /**
     * Adds multiple embeddings to the store and returns the generated identifiers.
     *
     * @param embeddings the list of embeddings to add; if null or empty, no changes are made and an empty list is returned
     * @param embedded   optional list of TextSegment objects aligned with `embeddings`; may be null or shorter than `embeddings`
     * @return           a list of generated IDs corresponding to the inserted embeddings (in the same order as `embeddings`)
     */
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

    /**
     * Deletes records with the given ids from this store's collection.
     *
     * @param ids collection of record ids to delete; if {@code null} or empty, no action is taken
     */
    @Override
    public void removeAll(java.util.Collection<String> ids) {
        if (ids == null || ids.isEmpty()) {
            return;
        }
        for (String id : ids) {
            driver.delete(new RecordId(collection, id));
        }
    }

    /**
     * Removes all records from the configured SurrealDB collection used by this embedding store.
     *
     * After this call the collection will contain no embedding records.
     */
    @Override
    public void removeAll() {
        driver.delete(collection);
    }

    /**
     * Deletes all records in the configured collection that match the provided filter.
     *
     * Maps the given Filter to a SurrealDB WHERE clause and executes a DELETE query for matching rows.
     *
     * @param filter the criteria used to select records to remove; must be translated to a SurrealDB WHERE clause
     */
    @Override
    public void removeAll(Filter filter) {
        Map<String, java.lang.Object> params = new HashMap<>();
        SurrealDbFilterMapper filterMapper = new SurrealDbFilterMapper(params);
        String whereClause = filterMapper.map(filter);
        String query = "DELETE " + collection + " WHERE " + whereClause;
        executeQuery(query, params);
    }

    /**
     * Persists a single embedding (with optional text and metadata) into the configured SurrealDB collection using the provided id.
     *
     * @param id           the identifier to store the record under
     * @param embedding    the embedding to persist
     * @param textSegment  optional text and metadata to attach to the stored record; may be null
     */
    private void addInternal(String id, Embedding embedding, TextSegment textSegment) {
        SurrealEmbeddingRecord record = new SurrealEmbeddingRecord(collection, id, embedding.vectorAsList(), textSegment);
        driver.create(new RecordId(collection, id), record);
    }

    /**
     * Execute a parameterized SurrealDB query using this store's driver.
     *
     * @param query  the SurrealDB query to execute
     * @param params a map of bind parameters for the query, or null if none
     * @return       the Response returned by the Surreal driver
     */
    private Response executeQuery(String query, Map<String, java.lang.Object> params) {
        return executeQuery(driver, query, params);
    }

    /**
     * Execute a parameterized SurrealDB query and return the raw response.
     *
     * If `params` is null, an empty parameter map is used.
     *
     * @param driver the SurrealDB driver to execute the query with
     * @param query the SurrealDB query string
     * @param params a map of parameters to bind into the query; may be null
     * @return the SurrealDB {@code Response} produced by the query
     */
    private static Response executeQuery(Surreal driver, String query, Map<String, java.lang.Object> params) {
        Map<String, java.lang.Object> safeParams = params == null ? Map.of() : params;
        return driver.queryBind(query, safeParams);
    }

    /**
     * Searches the store for embeddings nearest to the request embedding and returns ranked matches with optional text and metadata.
     *
     * @param request the search request containing the query embedding, optional filter, maximum results, and minimum score
     * @return an EmbeddingSearchResult containing matches ordered by descending similarity score
     * @throws RuntimeException if the query execution or result parsing fails
     */
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
            Response response = executeQuery(query, params);

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

        /**
         * Set the SurrealDB host to connect to.
         *
         * @param host the hostname or IP address of the SurrealDB server
         * @return this Builder instance for method chaining
         */
        public Builder host(String host) {
            this.host = host;
            return this;
        }

        /**
         * Sets the network port for connecting to the SurrealDB server.
         *
         * @param port the port number to use for the connection
         * @return this Builder instance
         */
        public Builder port(int port) {
            this.port = port;
            return this;
        }

        /**
         * Sets whether to use TLS when connecting to SurrealDB.
         *
         * @param useTls `true` to use TLS (wss), `false` to use plain WebSocket (ws)
         * @return this Builder instance
         */
        public Builder useTls(boolean useTls) {
            this.useTls = useTls;
            return this;
        }

        /**
         * Set the SurrealDB namespace to use when connecting.
         *
         * @param namespace the namespace identifier
         * @return this Builder instance for chaining
         */
        public Builder namespace(String namespace) {
            this.namespace = namespace;
            return this;
        }

        /**
         * Set the SurrealDB database name to connect to.
         *
         * @param database the database name
         * @return the builder instance
         */
        public Builder database(String database) {
            this.database = database;
            return this;
        }

        /**
         * Sets the username used to authenticate with the SurrealDB server.
         *
         * @param username the username for SurrealDB authentication; must be provided before calling {@link #build()}
         * @return this Builder instance
         */
        public Builder username(String username) {
            this.username = username;
            return this;
        }

        /**
         * Sets the password used for authenticating with the SurrealDB server.
         *
         * @param password the password for the SurrealDB user
         * @return this Builder instance
         */
        public Builder password(String password) {
            this.password = password;
            return this;
        }

        /**
         * Sets the SurrealDB collection (table) name used to store embeddings.
         *
         * @param collection the collection name; defaults to "vectors" if not set
         * @return this Builder instance
         */
        public Builder collection(String collection) {
            this.collection = collection;
            return this;
        }

        /**
         * Set the embedding vector dimensionality used when creating the HNSW index.
         *
         * @param dimension the number of dimensions for embeddings; must be greater than zero
         * @return this Builder instance
         */
        public Builder dimension(int dimension) {
            this.dimension = dimension;
            return this;
        }

        /**
         * Create and configure a SurrealDbEmbeddingStore using the builder's settings.
         *
         * Ensures a connection to the specified SurrealDB host, selects the configured
         * namespace and database, defines the collection table if missing, and ensures
         * an HNSW index on the `embedding` field with the configured dimension.
         *
         * @return a configured SurrealDbEmbeddingStore connected to the specified SurrealDB namespace and database
         * @throws NullPointerException if a required builder property (host, namespace, database, username, password) is null
         * @throws RuntimeException if the driver cannot connect or sign in, if defining the table or creating the index fails,
         *                          or if an existing index's dimension does not match the configured dimension
         */
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
            driver.signin(new Root(username, password));
            driver.useNs(namespace);
            driver.useDb(database);

            // Ensure the table exists before creating an index.
            String defineTableQuery = String.format("DEFINE TABLE %s SCHEMALESS;", collection);
            try {
                executeQuery(driver, defineTableQuery, null);
            } catch (Exception e) {
                String message = e.getMessage();
                if (message == null || !message.contains("already exists")) {
                    throw new RuntimeException("Failed to define table", e);
                }
            }

            // Create index if needed
            String indexQuery = String.format(
                    "DEFINE INDEX idx_embedding_%s ON TABLE %s FIELDS embedding HNSW DIMENSION %d DIST COSINE TYPE F32;",
                    collection, collection, dimension
            );

            try {
                executeQuery(driver, indexQuery, null);
            } catch (Exception e) {
                String message = e.getMessage();
                if (message == null || !message.contains("already exists")) {
                    throw new RuntimeException("Failed to create index", e);
                }
                validateExistingIndex(driver, collection, dimension);
            }

            return new SurrealDbEmbeddingStore(driver, namespace, database, collection, dimension);
        }

        /**
         * Validates that an existing HNSW embedding index for the given collection (if present) has the expected dimension.
         *
         * If no such index is found this method returns without action.
         *
         * @param driver the SurrealDB driver used to query table info
         * @param collection the collection (table) name whose indexes will be inspected
         * @param expectedDimension the required embedding dimension for the index
         * @throws RuntimeException if an existing embedding index reports a different dimension or if validation fails
         */
        private static void validateExistingIndex(Surreal driver, String collection, int expectedDimension) {
            try {
                Response response = executeQuery(driver, "INFO FOR TABLE " + collection + ";", null);
                Value value = response.take(0);
                if (value == null || !value.isObject()) {
                    return;
                }
                Value indexes = value.getObject().get("indexes");
                if (indexes == null) {
                    return;
                }
                String definition = null;
                String indexName = "idx_embedding_" + collection;
                if (indexes.isObject()) {
                    Value idxVal = indexes.getObject().get(indexName);
                    if (idxVal != null && idxVal.isString()) {
                        definition = idxVal.getString();
                    }
                } else if (indexes.isArray()) {
                    for (Value idx : indexes.getArray()) {
                        if (idx.isObject()) {
                            Value name = idx.getObject().get("name");
                            Value def = idx.getObject().get("index");
                            if (name != null && name.isString() && indexName.equals(name.getString())
                                    && def != null && def.isString()) {
                                definition = def.getString();
                                break;
                            }
                        }
                    }
                }
                if (definition == null) {
                    return;
                }
                java.util.regex.Matcher matcher = java.util.regex.Pattern.compile("DIMENSION\\s+(\\d+)").matcher(definition);
                if (matcher.find()) {
                    int actualDimension = Integer.parseInt(matcher.group(1));
                    if (actualDimension != expectedDimension) {
                        throw new RuntimeException("Existing index " + indexName
                                + " has dimension " + actualDimension + " but expected " + expectedDimension);
                    }
                }
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException("Failed to validate existing index", e);
            }
        }
    }

    // Helper class for serialization
    public static class SurrealEmbeddingRecord {
        public String collection;
        public String id;

        public List<Float> embedding;
        public String text;
        public Map<String, java.lang.Object> metadata = new HashMap<>();

        /**
         * Constructs a SurrealEmbeddingRecord for the given collection, id and embedding, optionally attaching text and metadata.
         *
         * @param collection the SurrealDB collection name
         * @param id the record identifier
         * @param embedding the embedding vector to store
         * @param textSegment optional TextSegment whose text and metadata will be extracted and stored; may be {@code null}
         */
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