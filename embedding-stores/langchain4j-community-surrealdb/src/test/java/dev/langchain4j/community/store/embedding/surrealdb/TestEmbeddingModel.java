package dev.langchain4j.community.store.embedding.surrealdb;

import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.model.embedding.EmbeddingModel;
import dev.langchain4j.model.output.Response;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Lightweight deterministic embedding model for tests to avoid heavyweight ONNX downloads.
 */
class TestEmbeddingModel implements EmbeddingModel {

    private final int dimension;

    TestEmbeddingModel(int dimension) {
        this.dimension = dimension;
    }

    @Override
    public Response<List<Embedding>> embedAll(List<TextSegment> textSegments) {
        List<Embedding> embeddings = new ArrayList<>(textSegments.size());
        for (TextSegment segment : textSegments) {
            embeddings.add(createEmbedding(segment.text()));
        }
        return Response.from(embeddings);
    }

    @Override
    public int dimension() {
        return dimension;
    }

    private Embedding createEmbedding(String text) {
        byte[] bytes = text == null ? new byte[0] : text.getBytes(StandardCharsets.UTF_8);
        float[] vector = new float[dimension];
        for (int i = 0; i < bytes.length; i++) {
            vector[i % dimension] += (bytes[i] & 0xFF) / 255.0f;
        }
        return Embedding.from(vector);
    }
}
