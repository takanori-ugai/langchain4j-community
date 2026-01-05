package dev.langchain4j.community.store.embedding.surrealdb;

import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.model.embedding.EmbeddingModel;
import dev.langchain4j.model.embedding.onnx.allminilml6v2q.AllMiniLmL6V2QuantizedEmbeddingModel;
import dev.langchain4j.store.embedding.EmbeddingStore;
import dev.langchain4j.store.embedding.EmbeddingStoreWithRemovalIT;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class SurrealDbEmbeddingStoreRemovalIT extends EmbeddingStoreWithRemovalIT {

    static final String USERNAME = "root";
    static final String PASSWORD = "root";
    static final String NAMESPACE = "test_ns";
    static final String DATABASE = "test_db";
    static final String COLLECTION = "test_vectors";

    @Container
    static GenericContainer<?> surrealdb = new GenericContainer<>("surrealdb/surrealdb:latest")
            .withExposedPorts(8000)
            .withCommand("start --log trace --user " + USERNAME + " --pass " + PASSWORD)
            .waitingFor(Wait.forLogMessage(".*Started web server on.*", 1));

    EmbeddingModel embeddingModel = new AllMiniLmL6V2QuantizedEmbeddingModel();

    SurrealDbEmbeddingStore embeddingStore;

    @BeforeAll
    static void beforeAll() {
        surrealdb.start();
    }

    @AfterAll
    static void afterAll() {
        surrealdb.stop();
    }

    @BeforeEach
    void beforeEach() {
        embeddingStore = SurrealDbEmbeddingStore.builder()
                .host(surrealdb.getHost())
                .port(surrealdb.getMappedPort(8000))
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
