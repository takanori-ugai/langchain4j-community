package dev.langchain4j.community.store.embedding.surrealdb;

import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.model.embedding.EmbeddingModel;
import dev.langchain4j.store.embedding.EmbeddingStore;
import dev.langchain4j.store.embedding.EmbeddingStoreWithRemovalIT;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

class SurrealDbEmbeddingStoreRemovalIT extends EmbeddingStoreWithRemovalIT {

    static final String USERNAME = SurrealDbEmbeddingStoreBaseTest.USERNAME;
    static final String PASSWORD = SurrealDbEmbeddingStoreBaseTest.PASSWORD;
    static final String NAMESPACE = SurrealDbEmbeddingStoreBaseTest.NAMESPACE;
    static final String DATABASE = SurrealDbEmbeddingStoreBaseTest.DATABASE;
    static final String COLLECTION = SurrealDbEmbeddingStoreBaseTest.COLLECTION;

    EmbeddingModel embeddingModel = new TestEmbeddingModel(SurrealDbEmbeddingStoreBaseTest.DIMENSION);

    SurrealDbEmbeddingStore embeddingStore;
    private static String host;
    private static int port;

    /**
     * Starts the SurrealDB test container and initializes the static host and port used by tests.
     *
     * This method runs once before all test cases to ensure the container is running and its
     * connection details are available via the class-level host and port fields.
     */
    @BeforeAll
    static void beforeAllTests() {
        SurrealDbEmbeddingStoreBaseTest.startContainer();
        host = SurrealDbEmbeddingStoreBaseTest.HOST;
        port = SurrealDbEmbeddingStoreBaseTest.PORT;
    }

    /**
     * Stops the SurrealDB test container started for the integration tests.
     */
    @AfterAll
    static void afterAllTests() {
        SurrealDbEmbeddingStoreBaseTest.stopContainer();
    }

    /**
     * Prepares a SurrealDbEmbeddingStore configured for tests and clears all embeddings before each test.
     *
     * <p>This initializes the test store using the test connection and configuration constants, then removes
     * any existing embeddings so each test starts with an empty store.
     */
    @BeforeEach
    void beforeEach() {
        embeddingStore = SurrealDbEmbeddingStore.builder()
                .host(host)
                .port(port)
                .useTls(false)
                .namespace(NAMESPACE)
                .database(DATABASE)
                .username(USERNAME)
                .password(PASSWORD)
                .collection(COLLECTION)
                .dimension(SurrealDbEmbeddingStoreBaseTest.DIMENSION)
                .build();
        embeddingStore.removeAll();
    }

    /**
     * Provides the configured embedding store instance used by the test.
     *
     * @return the configured {@code EmbeddingStore<TextSegment>} used by the tests
     */
    @Override
    protected EmbeddingStore<TextSegment> embeddingStore() {
        return embeddingStore;
    }

    /**
     * Provides the embedding model used by this test instance.
     *
     * @return the EmbeddingModel used to generate embeddings for tests
     */
    @Override
    protected EmbeddingModel embeddingModel() {
        return embeddingModel;
    }
}