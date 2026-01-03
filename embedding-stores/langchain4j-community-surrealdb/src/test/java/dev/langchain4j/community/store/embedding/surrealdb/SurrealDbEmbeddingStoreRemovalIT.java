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

    @BeforeAll
    static void beforeAllTests() {
        SurrealDbEmbeddingStoreBaseTest.startContainer();
        host = SurrealDbEmbeddingStoreBaseTest.HOST;
        port = SurrealDbEmbeddingStoreBaseTest.PORT;
    }

    @AfterAll
    static void afterAllTests() {
        SurrealDbEmbeddingStoreBaseTest.stopContainer();
    }

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

    @Override
    protected EmbeddingStore<TextSegment> embeddingStore() {
        return embeddingStore;
    }

    @Override
    protected EmbeddingModel embeddingModel() {
        return embeddingModel;
    }
}
