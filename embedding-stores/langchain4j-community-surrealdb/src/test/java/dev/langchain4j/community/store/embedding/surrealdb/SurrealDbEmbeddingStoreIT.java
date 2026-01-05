package dev.langchain4j.community.store.embedding.surrealdb;

import dev.langchain4j.store.embedding.EmbeddingStore;
import dev.langchain4j.data.segment.TextSegment;

public class SurrealDbEmbeddingStoreIT extends SurrealDbEmbeddingStoreBaseTest {

    @Override
    protected EmbeddingStore<TextSegment> embeddingStore() {
        return embeddingStore;
    }
}
