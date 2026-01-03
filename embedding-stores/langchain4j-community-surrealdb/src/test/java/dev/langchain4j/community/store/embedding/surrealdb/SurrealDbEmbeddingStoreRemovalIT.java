package dev.langchain4j.community.store.embedding.surrealdb;

import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.model.embedding.EmbeddingModel;
import dev.langchain4j.model.embedding.onnx.allminilml6v2q.AllMiniLmL6V2QuantizedEmbeddingModel;
import dev.langchain4j.store.embedding.EmbeddingStore;
import dev.langchain4j.store.embedding.EmbeddingStoreWithRemovalIT;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

class SurrealDbEmbeddingStoreRemovalIT extends EmbeddingStoreWithRemovalIT {

    static final String USERNAME = "root";
    static final String PASSWORD = "root";
    static final String NAMESPACE = "test_ns";
    static final String DATABASE = "test_db";
    static final String COLLECTION = "test_vectors";

    EmbeddingModel embeddingModel = new AllMiniLmL6V2QuantizedEmbeddingModel();

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
                .dimension(384)
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
