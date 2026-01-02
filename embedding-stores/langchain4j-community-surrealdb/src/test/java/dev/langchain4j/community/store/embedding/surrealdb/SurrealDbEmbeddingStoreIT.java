package dev.langchain4j.community.store.embedding.surrealdb;

import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.store.embedding.EmbeddingSearchRequest;
import dev.langchain4j.store.embedding.EmbeddingSearchResult;
import dev.langchain4j.store.embedding.EmbeddingStore;
import dev.langchain4j.store.embedding.EmbeddingStoreIT;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

public class SurrealDbEmbeddingStoreIT extends EmbeddingStoreIT {

    static GenericContainer<?> surrealdb = new GenericContainer<>("surrealdb/surrealdb:latest")
            .withExposedPorts(8000)
            .withCommand("start --log trace --user root --pass root")
            .waitingFor(Wait.forLogMessage(".*Started web server on.*", 1));

    static SurrealDbEmbeddingStore embeddingStore;

    @BeforeAll
    static void beforeAll() {
        surrealdb.start();
        embeddingStore = SurrealDbEmbeddingStore.builder()
                .host(surrealdb.getHost())
                .port(surrealdb.getMappedPort(8000))
                .useTls(false)
                .namespace("test_ns")
                .database("test_db")
                .username("root")
                .password("root")
                .collection("test_vectors")
                .dimension(384)
                .build();
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
    protected dev.langchain4j.model.embedding.EmbeddingModel embeddingModel() {
        return new dev.langchain4j.model.embedding.onnx.allminilml6v2q.AllMiniLmL6V2QuantizedEmbeddingModel();
    }
}
