package dev.langchain4j.community.store.embedding.surrealdb;

import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.model.embedding.EmbeddingModel;
import dev.langchain4j.store.embedding.EmbeddingStore;
import dev.langchain4j.store.embedding.EmbeddingStoreIT;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

public abstract class SurrealDbEmbeddingStoreBaseTest extends EmbeddingStoreIT {

    protected static final String USERNAME = "root";
    protected static final String PASSWORD = "root";
    protected static final String NAMESPACE = "test_ns";
    protected static final String DATABASE = "test_db";
    protected static final String COLLECTION = "test_vectors";
    protected static final int RPC_PORT = 8000;
    protected static final int DIMENSION = 8;
    protected static final String SURREALDB_IMAGE = System.getProperty("surrealdb.image", "surrealdb/surrealdb:latest");

    protected static String HOST;
    protected static int PORT;

    protected static final GenericContainer<?> surrealdbContainer =
            new GenericContainer<>(DockerImageName.parse(SURREALDB_IMAGE))
                    .withExposedPorts(RPC_PORT)
                    .withCommand("start", "--log", "info", "--user", USERNAME, "--pass", PASSWORD, "memory")
                    .waitingFor(
                            Wait.forLogMessage(".*Started web server on.*", 1)
                                    .withStartupTimeout(Duration.ofSeconds(60)));

    static {
        try {
            var jnaDir = Paths.get("target", "jna-tmp").toAbsolutePath();
            var djlDir = Paths.get("target", "djl-cache").toAbsolutePath();
            Files.createDirectories(jnaDir);
            Files.createDirectories(djlDir);
            System.setProperty("jna.tmpdir", jnaDir.toString());
            System.setProperty("DJL_CACHE_DIR", djlDir.toString());
        } catch (IOException e) {
            throw new RuntimeException("Failed to prepare temp directories", e);
        }
    }

    protected SurrealDbEmbeddingStore embeddingStore;

    protected final EmbeddingModel embeddingModel = new TestEmbeddingModel(DIMENSION);

    @BeforeAll
    static void beforeAll() {
        startContainer();
    }

    @AfterAll
    static void afterAll() {
        stopContainer();
    }

    protected static synchronized void startContainer() {
        if (!surrealdbContainer.isRunning()) {
            surrealdbContainer.start();
        }
        HOST = surrealdbContainer.getHost();
        PORT = surrealdbContainer.getMappedPort(RPC_PORT);
    }

    protected static synchronized void stopContainer() {
        if (surrealdbContainer != null && surrealdbContainer.isRunning()) {
            surrealdbContainer.stop();
        }
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
                .host(HOST)
                .port(PORT)
                .useTls(false)
                .namespace(NAMESPACE)
                .database(DATABASE)
                .username(USERNAME)
                .password(PASSWORD)
                .collection(COLLECTION)
                .dimension(DIMENSION)
                .build();
    }
}
