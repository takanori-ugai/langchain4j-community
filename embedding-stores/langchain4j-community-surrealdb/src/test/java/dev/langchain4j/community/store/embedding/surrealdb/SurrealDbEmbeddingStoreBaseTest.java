package dev.langchain4j.community.store.embedding.surrealdb;

import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.model.embedding.EmbeddingModel;
import dev.langchain4j.model.embedding.onnx.allminilml6v2q.AllMiniLmL6V2QuantizedEmbeddingModel;
import dev.langchain4j.store.embedding.EmbeddingStore;
import dev.langchain4j.store.embedding.EmbeddingStoreIT;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public abstract class SurrealDbEmbeddingStoreBaseTest extends EmbeddingStoreIT {

    protected static final String USERNAME = "root";
    protected static final String PASSWORD = "root";
    protected static final String NAMESPACE = "test_ns";
    protected static final String DATABASE = "test_db";
    protected static final String COLLECTION = "test_vectors";

    @Container
    protected static GenericContainer<?> surrealdb = new GenericContainer<>("surrealdb/surrealdb:latest")
            .withExposedPorts(8000)
            .withCommand("start --log trace --user " + USERNAME + " --pass " + PASSWORD)
            .waitingFor(Wait.forLogMessage(".*Started web server on.*", 1));

    protected SurrealDbEmbeddingStore embeddingStore;

    protected final EmbeddingModel embeddingModel = new AllMiniLmL6V2QuantizedEmbeddingModel();

    @BeforeAll
    static void beforeAll() {
        surrealdb.start();
    }

    @AfterAll
    static void afterAll() {
        surrealdb.stop();
    }

    @Override
    protected EmbeddingStore<TextSegment> embeddingStore() {
        return embeddingStore;
    }

    @Override
    protected EmbeddingModel embeddingModel() {
        return embeddingModel;
    }

    @Override
    protected void clearStore() {
        if (embeddingStore != null) {
            embeddingStore.removeAll();
        }

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
    }
}
