package dev.langchain4j.community.store.embedding.surrealdb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.surrealdb.Response;
import com.surrealdb.Surreal;
import com.surrealdb.Value;
import com.surrealdb.signin.Root;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class SurrealDbEmbeddingStoreSchemaCreationTest extends SurrealDbEmbeddingStoreBaseTest {

    @Test
    void should_create_hnsw_index_if_not_existing() {
        String collection = "schema_test_1";

        SurrealDbEmbeddingStore.builder()
                .host(HOST)
                .port(PORT)
                .useTls(false)
                .namespace(NAMESPACE)
                .database(DATABASE)
                .username(USERNAME)
                .password(PASSWORD)
                .collection(collection)
                .dimension(DIMENSION)
                .build();

        List<String> indexes = listIndexes(collection);

        assertThat(indexes).contains("idx_embedding_" + collection);
    }

    @Test
    void should_not_fail_if_index_exists() {
        String collection = "schema_test_2";
        String indexName = "idx_embedding_" + collection;

        executeAdminQuery(String.format(
                "DEFINE INDEX %s ON TABLE %s FIELDS embedding HNSW DIMENSION %d DIST COSINE TYPE F32;",
                indexName, collection, DIMENSION));

        assertThatCode(() -> SurrealDbEmbeddingStore.builder()
                        .host(HOST)
                        .port(PORT)
                        .useTls(false)
                        .namespace(NAMESPACE)
                .database(DATABASE)
                .username(USERNAME)
                .password(PASSWORD)
                .collection(collection)
                .dimension(DIMENSION)
                .build())
                .doesNotThrowAnyException();

        List<String> indexes = listIndexes(collection);
        assertThat(indexes).contains(indexName);
    }

    @Test
    void should_fail_when_existing_index_has_different_dimension() {
        String collection = "schema_test_conflict";
        String indexName = "idx_embedding_" + collection;
        executeAdminQuery(String.format(
                "DEFINE TABLE %s SCHEMALESS; DEFINE INDEX %s ON TABLE %s FIELDS embedding HNSW DIMENSION 16 DIST COSINE TYPE F32;",
                collection, indexName, collection));

        assertThatCode(() -> SurrealDbEmbeddingStore.builder()
                        .host(HOST)
                        .port(PORT)
                        .useTls(false)
                        .namespace(NAMESPACE)
                        .database(DATABASE)
                        .username(USERNAME)
                        .password(PASSWORD)
                        .collection(collection)
                        .dimension(DIMENSION)
                        .build())
                .isInstanceOf(RuntimeException.class);
    }

    private void executeAdminQuery(String query) {
        Surreal client = new Surreal();
        try {
            String connectString = String.format("ws://%s:%d/rpc", HOST, PORT);
            client.connect(connectString);
            client.signin(new Root(USERNAME, PASSWORD));
            client.useNs(NAMESPACE);
            client.useDb(DATABASE);
            client.query(query);
        } finally {
            client.close();
        }
    }

    private List<String> listIndexes(String collection) {
        Surreal client = new Surreal();
        try {
            String connectString = String.format("ws://%s:%d/rpc", HOST, PORT);
            client.connect(connectString);
            client.signin(new Root(USERNAME, PASSWORD));
            client.useNs(NAMESPACE);
            client.useDb(DATABASE);
            Response response = client.query("INFO FOR TABLE " + collection + ";");
            Value result = response.take(0);
            if (!result.isObject()) {
                return List.of();
            }
            Value indexes = result.getObject().get("indexes");
            if (indexes == null) {
                return List.of();
            }
            List<String> names = new ArrayList<>();
            if (indexes.isArray()) {
                for (Value index : indexes.getArray()) {
                    if (index.isObject()) {
                        Value name = index.getObject().get("name");
                        if (name != null && name.isString()) {
                            names.add(name.getString());
                        }
                    }
                }
            } else if (indexes.isObject()) {
                com.surrealdb.Object indexObj = indexes.getObject();
                for (com.surrealdb.Entry entry : indexObj) {
                    names.add(entry.getKey());
                }
            }
            return names;
        } finally {
            client.close();
        }
    }
}
