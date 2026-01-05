package dev.langchain4j.community.store.embedding.surrealdb;

import dev.langchain4j.store.embedding.filter.Filter;
import dev.langchain4j.store.embedding.filter.comparison.*;
import dev.langchain4j.store.embedding.filter.logical.And;
import dev.langchain4j.store.embedding.filter.logical.Not;
import dev.langchain4j.store.embedding.filter.logical.Or;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Maps {@link Filter} to SurrealQL `WHERE` clause.
 */
class SurrealDbFilterMapper {

    private static final Pattern VALID_KEY_PATTERN = Pattern.compile("^[a-zA-Z0-9_.]+$");

    private final Map<String, Object> parameters;
    private final AtomicInteger paramCounter = new AtomicInteger(0);

    /**
     * Creates a new instance of {@link SurrealDbFilterMapper}.
     *
     * @param parameters The map to store parameters in.
     */
    SurrealDbFilterMapper(Map<String, Object> parameters) {
        this.parameters = parameters;
    }

    /**
     * Maps the given filter to a SurrealQL `WHERE` clause.
     *
     * @param filter The filter to map.
     * @return The SurrealQL `WHERE` clause.
     */
    String map(Filter filter) {
        if (filter instanceof IsEqualTo) {
            return map((IsEqualTo) filter);
        } else if (filter instanceof IsNotEqualTo) {
            return map((IsNotEqualTo) filter);
        } else if (filter instanceof IsGreaterThan) {
            return map((IsGreaterThan) filter);
        } else if (filter instanceof IsGreaterThanOrEqualTo) {
            return map((IsGreaterThanOrEqualTo) filter);
        } else if (filter instanceof IsLessThan) {
            return map((IsLessThan) filter);
        } else if (filter instanceof IsLessThanOrEqualTo) {
            return map((IsLessThanOrEqualTo) filter);
        } else if (filter instanceof IsIn) {
            return map((IsIn) filter);
        } else if (filter instanceof IsNotIn) {
            return map((IsNotIn) filter);
        } else if (filter instanceof And) {
            return map((And) filter);
        } else if (filter instanceof Or) {
            return map((Or) filter);
        } else if (filter instanceof Not) {
            return map((Not) filter);
        } else {
            throw new UnsupportedOperationException("Unsupported filter type: " + filter.getClass().getName());
        }
    }

    private String map(IsEqualTo filter) {
        String paramName = createParam(filter.comparisonValue());
        return String.format("metadata.%s = $%s", validateKey(filter.key()), paramName);
    }

    private String map(IsNotEqualTo filter) {
        String paramName = createParam(filter.comparisonValue());
        return String.format("metadata.%s != $%s", validateKey(filter.key()), paramName);
    }

    private String map(IsGreaterThan filter) {
        String paramName = createParam(filter.comparisonValue());
        return String.format("metadata.%s > $%s", validateKey(filter.key()), paramName);
    }

    private String map(IsGreaterThanOrEqualTo filter) {
        String paramName = createParam(filter.comparisonValue());
        return String.format("metadata.%s >= $%s", validateKey(filter.key()), paramName);
    }

    private String map(IsLessThan filter) {
        String paramName = createParam(filter.comparisonValue());
        return String.format("metadata.%s < $%s", validateKey(filter.key()), paramName);
    }

    private String map(IsLessThanOrEqualTo filter) {
        String paramName = createParam(filter.comparisonValue());
        return String.format("metadata.%s <= $%s", validateKey(filter.key()), paramName);
    }

    private String map(IsIn filter) {
        String paramName = createParam(filter.comparisonValues());
        return String.format("metadata.%s INSIDE $%s", validateKey(filter.key()), paramName);
    }

    private String map(IsNotIn filter) {
        String paramName = createParam(filter.comparisonValues());
        return String.format("metadata.%s NOTINSIDE $%s", validateKey(filter.key()), paramName);
    }

    private String map(And filter) {
        return String.format("(%s) AND (%s)", map(filter.left()), map(filter.right()));
    }

    private String map(Or filter) {
        return String.format("(%s) OR (%s)", map(filter.left()), map(filter.right()));
    }

    private String map(Not filter) {
        return String.format("!(%s)", map(filter.expression()));
    }

    private String createParam(Object value) {
        String name = "filter_param_" + paramCounter.getAndIncrement();
        parameters.put(name, value);
        return name;
    }

    private String validateKey(String key) {
        if (!VALID_KEY_PATTERN.matcher(key).matches()) {
            throw new IllegalArgumentException("Invalid key: " + key + ". Only alphanumeric characters, underscores, and dots are allowed.");
        }
        return key;
    }
}
