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

    /**
     * Creates a TestEmbeddingModel that produces deterministic embeddings of the given length.
     *
     * @param dimension the length of each embedding vector produced by this model (number of float components)
     */
    TestEmbeddingModel(int dimension) {
        this.dimension = dimension;
    }

    /**
     * Produces embeddings for each provided text segment in order.
     *
     * @param textSegments the list of text segments to embed; order is preserved
     * @return a Response containing a list of Embedding objects corresponding to the input segments in the same order
     */
    @Override
    public Response<List<Embedding>> embedAll(List<TextSegment> textSegments) {
        List<Embedding> embeddings = new ArrayList<>(textSegments.size());
        for (TextSegment segment : textSegments) {
            embeddings.add(createEmbedding(segment.text()));
        }
        return Response.from(embeddings);
    }

    /**
     * Provides the embedding vector length produced by this model.
     *
     * @return the number of dimensions in each embedding vector
     */
    @Override
    public int dimension() {
        return dimension;
    }

    /**
     * Builds a deterministic embedding vector from the UTF-8 bytes of the given text.
     *
     * <p>Null text is treated as empty. Bytes are converted to unsigned values and accumulated
     * into a float vector of length {@code dimension} in round-robin order; each byte contributes
     * (byte &amp; 0xFF) / 255.0f to the corresponding vector slot. The resulting vector is
     * wrapped with {@link Embedding#from(float[])}.
     *
     * @param text the input text to embed, or {@code null} to produce an all-zero embedding
     * @return an {@link Embedding} constructed from the computed float vector of length {@code dimension}
     */
    private Embedding createEmbedding(String text) {
        byte[] bytes = text == null ? new byte[0] : text.getBytes(StandardCharsets.UTF_8);
        float[] vector = new float[dimension];
        for (int i = 0; i < bytes.length; i++) {
            vector[i % dimension] += (bytes[i] & 0xFF) / 255.0f;
        }
        return Embedding.from(vector);
    }
}