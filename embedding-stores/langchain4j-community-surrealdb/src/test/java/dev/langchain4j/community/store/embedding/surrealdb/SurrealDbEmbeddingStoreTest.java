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
import dev.langchain4j.store.embedding.filter.comparison.IsIn;
import dev.langchain4j.store.embedding.filter.comparison.IsNotEqualTo;
import dev.langchain4j.store.embedding.filter.logical.And;
import dev.langchain4j.store.embedding.filter.logical.Not;
import dev.langchain4j.store.embedding.filter.logical.Or;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SurrealDbEmbeddingStoreTest extends SurrealDbEmbeddingStoreBaseTest {

    @BeforeEach
    void clean() {
        embeddingStore.removeAll();
    }

    @Test
    void should_add_embedding_and_return_match() {
        Embedding embedding = embeddingModel.embed("embedText").content();

        String id = embeddingStore.add(embedding);
        assertThat(id).isNotBlank();

        EmbeddingSearchRequest request = EmbeddingSearchRequest.builder()
                .queryEmbedding(embedding)
                .maxResults(5)
                .build();

        List<EmbeddingMatch<TextSegment>> matches = embeddingStore.search(request).matches();
        assertThat(matches).hasSize(1);
        assertThat(matches.get(0).embeddingId()).isEqualTo(id);
    }

    @Test
    void should_add_embedding_with_segment_and_metadata() {
        Metadata metadata = Metadata.from("category", "news");
        TextSegment segment = TextSegment.from("hello world", metadata);
        Embedding embedding = embeddingModel.embed(segment.text()).content();

        String id = embeddingStore.add(embedding, segment);
        assertThat(id).isNotBlank();

        EmbeddingSearchRequest request = EmbeddingSearchRequest.builder()
                .queryEmbedding(embedding)
                .maxResults(5)
                .build();

        List<EmbeddingMatch<TextSegment>> matches = embeddingStore.search(request).matches();
        assertThat(matches).hasSize(1);
        EmbeddingMatch<TextSegment> match = matches.get(0);
        assertThat(match.embeddingId()).isEqualTo(id);
        assertThat(match.embedded().metadata().toMap().get("category")).isEqualTo("news");
    }

    @Test
    void should_add_multiple_embeddings_and_return_all() {
        List<TextSegment> segments = List.of(
                TextSegment.from("first"),
                TextSegment.from("second"));
        List<Embedding> embeddings = embeddingModel.embedAll(segments).content();

        List<String> ids = embeddingStore.addAll(embeddings, segments);
        assertThat(ids).hasSize(segments.size());

        EmbeddingSearchRequest request = EmbeddingSearchRequest.builder()
                .queryEmbedding(embeddingModel.embed("first").content())
                .maxResults(5)
                .build();

        List<EmbeddingMatch<TextSegment>> matches = embeddingStore.search(request).matches();
        assertThat(matches).hasSize(2);
    }

    @Test
    void should_filter_with_metadata_predicates() {
        List<TextSegment> segments = IntStream.range(0, 5)
                .mapToObj(i -> {
                    Map<String, Object> meta = Map.of("group", i % 2 == 0 ? "even" : "odd", "index", String.valueOf(i));
                    return TextSegment.from("text-" + i, Metadata.from(meta));
                })
                .toList();
        List<Embedding> embeddings = embeddingModel.embedAll(segments).content();
        embeddingStore.addAll(embeddings, segments);

        Filter filter = new And(
                new IsEqualTo("group", "even"),
                new Not(new Or(
                        new IsIn("index", List.of("1", "3")),
                        new IsNotEqualTo("group", "even"))));

        EmbeddingSearchRequest request = EmbeddingSearchRequest.builder()
                .queryEmbedding(embeddingModel.embed("text-0").content())
                .maxResults(5)
                .filter(filter)
                .build();

        List<EmbeddingMatch<TextSegment>> matches = embeddingStore.search(request).matches();
        assertThat(matches).hasSize(3);
        matches.forEach(match -> assertThat(match.embedded().metadata().toMap().get("group")).isEqualTo("even"));
    }

    @Test
    void should_add_embeddings_with_custom_ids_and_retrieve() {
        List<TextSegment> segments = List.of(
                TextSegment.from("custom-1", Metadata.from("k", "v1")),
                TextSegment.from("custom-2", Metadata.from("k", "v2")));
        List<Embedding> embeddings = embeddingModel.embedAll(segments).content();
        List<String> ids = List.of("id-1", "id-2");

        embeddingStore.addAll(ids, embeddings, segments);

        EmbeddingSearchRequest request = EmbeddingSearchRequest.builder()
                .queryEmbedding(embeddingModel.embed("custom").content())
                .maxResults(2)
                .build();

        List<EmbeddingMatch<TextSegment>> matches = embeddingStore.search(request).matches();
        assertThat(matches).hasSize(2);
        assertThat(matches.stream().map(EmbeddingMatch::embeddingId)).containsExactlyInAnyOrder("id-1", "id-2");
    }

    @Override
    protected EmbeddingStore<TextSegment> embeddingStore() {
        return embeddingStore;
    }

    @Test
    void should_respect_min_score() {
        TextSegment segment1 = TextSegment.from("foo bar");
        TextSegment segment2 = TextSegment.from("lorem ipsum");
        List<TextSegment> segments = List.of(segment1, segment2);
        List<Embedding> embeddings = embeddingModel.embedAll(segments).content();
        embeddingStore.addAll(embeddings, segments);

        Embedding query = embeddingModel.embed("foo").content();
        EmbeddingSearchRequest lowThreshold = EmbeddingSearchRequest.builder()
                .queryEmbedding(query)
                .maxResults(2)
                .minScore(0.0)
                .build();
        EmbeddingSearchRequest highThreshold = EmbeddingSearchRequest.builder()
                .queryEmbedding(query)
                .maxResults(2)
                .minScore(0.99)
                .build();

        assertThat(embeddingStore.search(lowThreshold).matches()).hasSize(2);
        assertThat(embeddingStore.search(highThreshold).matches()).hasSize(1);
    }

    @Test
    void should_handle_large_batch_inserts() {
        int count = 2000;
        List<TextSegment> segments = IntStream.range(0, count)
                .mapToObj(i -> TextSegment.from("text-" + i, Metadata.from("i", i)))
                .toList();
        List<Embedding> embeddings = embeddingModel.embedAll(segments).content();
        embeddingStore.addAll(embeddings, segments);

        Embedding query = embeddingModel.embed("text-1999").content();
        EmbeddingSearchRequest request = EmbeddingSearchRequest.builder()
                .queryEmbedding(query)
                .maxResults(5)
                .build();

        List<EmbeddingMatch<TextSegment>> matches = embeddingStore.search(request).matches();
        assertThat(matches).isNotEmpty();
    }
}
