package dev.langchain4j.community.store.embedding.surrealdb;

import dev.langchain4j.store.embedding.filter.Filter;
import dev.langchain4j.store.embedding.filter.comparison.*;
import dev.langchain4j.store.embedding.filter.logical.And;
import dev.langchain4j.store.embedding.filter.logical.Not;
import dev.langchain4j.store.embedding.filter.logical.Or;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class SurrealDbFilterMapperTest {

    private Map<String, Object> params;
    private SurrealDbFilterMapper mapper;

    @BeforeEach
    void setUp() {
        params = new HashMap<>();
        mapper = new SurrealDbFilterMapper(params);
    }

    @Test
    void should_map_equal() {
        IsEqualTo filter = new IsEqualTo("key", "value");
        String clause = mapper.map(filter);
        assertThat(clause).startsWith("metadata.key = $");
        String paramName = clause.substring(clause.indexOf("$") + 1);
        assertThat(params).containsEntry(paramName, "value");
    }

    @Test
    void should_map_not_equal() {
        IsNotEqualTo filter = new IsNotEqualTo("key", 123);
        String clause = mapper.map(filter);
        assertThat(clause).startsWith("metadata.key != $");
        String paramName = clause.substring(clause.indexOf("$") + 1);
        assertThat(params).containsEntry(paramName, 123);
    }

    @Test
    void should_map_greater_than() {
        IsGreaterThan filter = new IsGreaterThan("age", 18);
        String clause = mapper.map(filter);
        assertThat(clause).startsWith("metadata.age > $");
        String paramName = clause.substring(clause.indexOf("$") + 1);
        assertThat(params).containsEntry(paramName, 18);
    }

    @Test
    void should_map_in() {
        Collection<String> values = Set.of("a", "b");
        IsIn filter = new IsIn("category", values);
        String clause = mapper.map(filter);
        assertThat(clause).startsWith("metadata.category INSIDE $");
        String paramName = clause.substring(clause.indexOf("$") + 1);
        assertThat(params).containsKey(paramName);
        assertThat((Collection<String>) params.get(paramName)).containsExactlyInAnyOrder("a", "b");
    }

    @Test
    void should_map_and() {
        And filter = new And(new IsEqualTo("a", 1), new IsEqualTo("b", 2));
        String clause = mapper.map(filter);
        assertThat(clause).matches("\\(metadata.a = \\$filter_param_\\d+\\) AND \\(metadata.b = \\$filter_param_\\d+\\)");
        assertThat(params).hasSize(2);
    }

    @Test
    void should_map_or() {
        Or filter = new Or(new IsEqualTo("a", 1), new IsNotEqualTo("b", 2));
        String clause = mapper.map(filter);
        assertThat(clause).matches("\\(metadata.a = \\$filter_param_\\d+\\) OR \\(metadata.b != \\$filter_param_\\d+\\)");
        assertThat(params).hasSize(2);
    }

    @Test
    void should_map_not() {
        Not filter = new Not(new IsGreaterThan("score", 10));
        String clause = mapper.map(filter);
        assertThat(clause).matches("!\\(metadata.score > \\$filter_param_\\d+\\)");
        assertThat(params).containsValue(10);
    }

    @Test
    void should_map_not_in() {
        Collection<String> values = Set.of("x", "y");
        IsNotIn filter = new IsNotIn("tag", values);
        String clause = mapper.map(filter);
        assertThat(clause).startsWith("metadata.tag NOTINSIDE $");
        String paramName = clause.substring(clause.indexOf("$") + 1);
        assertThat(params.get(paramName)).isEqualTo(values);
    }

    @Test
    void should_map_greater_than_or_equal_and_less_than_or_equal() {
        IsGreaterThanOrEqualTo gte = new IsGreaterThanOrEqualTo("age", 21);
        IsLessThanOrEqualTo lte = new IsLessThanOrEqualTo("age", 65);
        String gteClause = mapper.map(gte);
        String lteClause = mapper.map(lte);
        assertThat(gteClause).contains(">=");
        assertThat(lteClause).contains("<=");
        assertThat(params.values()).contains(21, 65);
    }
}
