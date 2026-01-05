package dev.langchain4j.community.store.embedding.surrealdb;

import static org.assertj.core.api.Assertions.assertThat;

import dev.langchain4j.data.document.Metadata;
import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.store.embedding.EmbeddingMatch;
import dev.langchain4j.store.embedding.EmbeddingSearchRequest;
import dev.langchain4j.store.embedding.EmbeddingStore;
import dev.langchain4j.store.embedding.filter.Filter;
import dev.langchain4j.store.embedding.filter.comparison.IsEqualTo;
import org.junit.jupiter.api.Assertions;
import java.util.List;
import org.junit.jupiter.api.Test;

public class SurrealDbEmbeddingStoreIT extends SurrealDbEmbeddingStoreBaseTest {

    /**
     * Provide the embedding store instance used by the test class.
     *
     * @return the concrete embedding store instance used for testing
     */
    @Override
    protected EmbeddingStore<TextSegment> embeddingStore() {
        return embeddingStore;
    }

    /**
     * Integration test that verifies filtered and unfiltered embedding searches return the expected matches.
     *
     * <p>Populates the store with three text segments labeled by category, adds their embeddings, then:
     * - Executes a filtered nearest-neighbor search for "hello" constrained to category = "news" and expects a single
     *   match with id "doc1" and metadata category "news".
     * - Executes an unfiltered search for "Elizabeth" and expects a match with id "doc2".</p>
     */
    @Test
    void should_emulate_issue_1306_case() {
        embeddingStore.removeAll();

        TextSegment doc1 = TextSegment.from("hello world", Metadata.from("category", "news"));
        TextSegment doc2 = TextSegment.from("Elizabeth I was queen", Metadata.from("category", "history"));
        TextSegment doc3 = TextSegment.from("random blog post", Metadata.from("category", "blog"));

        List<TextSegment> segments = List.of(doc1, doc2, doc3);
        List<Embedding> embeddings = embeddingModel.embedAll(segments).content();
        List<String> ids = List.of("doc1", "doc2", "doc3");

        embeddingStore.addAll(ids, embeddings, segments);

        Embedding queryEmbedding = embeddingModel.embed("hello").content();
        Filter filter = new IsEqualTo("category", "news");
        EmbeddingSearchRequest request = EmbeddingSearchRequest.builder()
                .queryEmbedding(queryEmbedding)
                .maxResults(2)
                .filter(filter)
                .build();

        List<EmbeddingMatch<TextSegment>> matches = embeddingStore.search(request).matches();

        assertThat(matches).hasSize(1);
        EmbeddingMatch<TextSegment> match = matches.get(0);
        assertThat(match.embeddingId()).isEqualTo("doc1");
        assertThat(match.embedded().metadata().toMap().get("category")).isEqualTo("news");

        Embedding fullTextQuery = embeddingModel.embed("Elizabeth").content();
        EmbeddingSearchRequest fullTextRequest = EmbeddingSearchRequest.builder()
                .queryEmbedding(fullTextQuery)
                .maxResults(3)
                .build();
        List<EmbeddingMatch<TextSegment>> fullTextMatches = embeddingStore.search(fullTextRequest).matches();
        assertThat(fullTextMatches).anySatisfy(m -> assertThat(m.embeddingId()).isEqualTo("doc2"));
    }

    @Test
    void should_throw_error_if_index_has_different_dimension() {
        embeddingStore.removeAll();

        // Pre-create an index with a different dimension
        SurrealDbEmbeddingStore.builder()
                .host(HOST)
                .port(PORT)
                .useTls(false)
                .namespace(NAMESPACE)
                .database(DATABASE)
                .username(USERNAME)
                .password(PASSWORD)
                .collection(COLLECTION + "_conflict")
                .dimension(DIMENSION + 2)
                .build();

        Assertions.assertThrows(RuntimeException.class, () -> SurrealDbEmbeddingStore.builder()
                .host(HOST)
                .port(PORT)
                .useTls(false)
                .namespace(NAMESPACE)
                .database(DATABASE)
                .username(USERNAME)
                .password(PASSWORD)
                .collection(COLLECTION + "_conflict")
                .dimension(DIMENSION)
                .build());
    }

    @Test
    void should_return_empty_when_filter_excludes_all_then_find_without_filter() {
        embeddingStore.removeAll();

        TextSegment doc1 = TextSegment.from("surrealdb is fun", Metadata.from("tag", "db"));
        TextSegment doc2 = TextSegment.from("vector search works", Metadata.from("tag", "search"));
        List<TextSegment> segments = List.of(doc1, doc2);
        List<Embedding> embeddings = embeddingModel.embedAll(segments).content();
        embeddingStore.addAll(embeddings, segments);

        Embedding query = embeddingModel.embed("surrealdb").content();
        Filter excludingFilter = new IsEqualTo("tag", "nonexistent");

        EmbeddingSearchRequest filtered = EmbeddingSearchRequest.builder()
                .queryEmbedding(query)
                .maxResults(2)
                .filter(excludingFilter)
                .build();
        EmbeddingSearchRequest unfiltered = EmbeddingSearchRequest.builder()
                .queryEmbedding(query)
                .maxResults(2)
                .build();

        assertThat(embeddingStore.search(filtered).matches()).isEmpty();
        assertThat(embeddingStore.search(unfiltered).matches()).isNotEmpty();
    }
}