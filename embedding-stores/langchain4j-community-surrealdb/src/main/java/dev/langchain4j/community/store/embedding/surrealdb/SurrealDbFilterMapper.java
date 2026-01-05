package dev.langchain4j.community.store.embedding.surrealdb;

import dev.langchain4j.store.embedding.filter.Filter;
import dev.langchain4j.store.embedding.filter.comparison.*;
import dev.langchain4j.store.embedding.filter.logical.And;
import dev.langchain4j.store.embedding.filter.logical.Not;
import dev.langchain4j.store.embedding.filter.logical.Or;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

class SurrealDbFilterMapper {

    private final Map<String, Object> parameters;
    private final AtomicInteger paramCounter = new AtomicInteger(0);

    /**
     * Initializes a SurrealDbFilterMapper that will populate the given parameter map with generated
     * query parameter bindings.
     *
     * @param parameters a mutable map where generated parameter names (e.g. "filter_param_0") will be
     *                   stored with their corresponding values for later use in a SurrealDB query
     */
    SurrealDbFilterMapper(Map<String, Object> parameters) {
        this.parameters = parameters;
    }

    /**
     * Maps a Filter node to its SurrealDB-compatible predicate fragment.
     *
     * @param filter the filter node to map; supported types: IsEqualTo, IsNotEqualTo, IsGreaterThan,
     *               IsGreaterThanOrEqualTo, IsLessThan, IsLessThanOrEqualTo, IsIn, IsNotIn, And, Or, Not
     * @return the SurrealDB predicate fragment representing the provided filter
     * @throws UnsupportedOperationException if the filter type is not supported
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

    /**
     * Converts an IsEqualTo filter into a SurrealDB equality predicate against the `metadata` object.
     *
     * Generates a unique parameter for the filter's comparison value (stored in the mapper's parameters)
     * and returns a predicate string of the form `metadata.{key} = $param`.
     *
     * @param filter the equality filter to map
     * @return the SurrealDB predicate string with a `$`-prefixed parameter placeholder
     */
    private String map(IsEqualTo filter) {
        String paramName = createParam(filter.comparisonValue());
        return String.format("metadata.%s = $%s", filter.key(), paramName);
    }

    /**
     * Maps an IsNotEqualTo filter to a SurrealDB predicate comparing a metadata field for inequality.
     *
     * @param filter the IsNotEqualTo filter containing the metadata key and comparison value
     * @return the predicate string in the form "metadata.{key} != $paramName" where `$paramName` is a generated parameter placeholder
     */
    private String map(IsNotEqualTo filter) {
        String paramName = createParam(filter.comparisonValue());
        return String.format("metadata.%s != $%s", filter.key(), paramName);
    }

    /**
     * Map an IsGreaterThan filter to a SurrealDB comparison expression and register its comparison value as a query parameter.
     *
     * @param filter the IsGreaterThan filter to convert
     * @return the SurrealDB predicate string in the form `metadata.{key} > ${param}` where the comparison value is stored under the generated parameter name
     */
    private String map(IsGreaterThan filter) {
        String paramName = createParam(filter.comparisonValue());
        return String.format("metadata.%s > $%s", filter.key(), paramName);
    }

    /**
     * Builds a SurrealDB predicate that tests whether the metadata field specified by the filter
     * is greater than or equal to the filter's comparison value.
     *
     * @return a predicate string in the form "metadata.{key} >= ${paramName}", where `{key}` is
     *         the filter key and `${paramName}` is a generated parameter name placed into the
     *         mapper's parameter bindings.
     */
    private String map(IsGreaterThanOrEqualTo filter) {
        String paramName = createParam(filter.comparisonValue());
        return String.format("metadata.%s >= $%s", filter.key(), paramName);
    }

    /**
     * Convert an IsLessThan filter into a SurrealDB predicate.
     *
     * Stores the filter's comparison value as a generated parameter in the mapper's parameters map and returns
     * the predicate referencing that parameter.
     *
     * @return the SurrealDB expression in the form "metadata.{key} < ${paramName}" where `{paramName}` is the generated parameter name
     */
    private String map(IsLessThan filter) {
        String paramName = createParam(filter.comparisonValue());
        return String.format("metadata.%s < $%s", filter.key(), paramName);
    }

    /**
     * Maps an IsLessThanOrEqualTo filter to a SurrealDB predicate comparing a metadata field to a bound parameter.
     *
     * The comparison value is registered in the mapper's parameter map under a generated name.
     *
     * @param filter the IsLessThanOrEqualTo filter to convert
     * @return the predicate string in the form "metadata.<key> <= $<paramName>"
     */
    private String map(IsLessThanOrEqualTo filter) {
        String paramName = createParam(filter.comparisonValue());
        return String.format("metadata.%s <= $%s", filter.key(), paramName);
    }

    /**
     * Map an IsIn filter to a SurrealDB `INSIDE` predicate and register its comparison values as a query parameter.
     *
     * The comparison values are stored in the mapper's parameters map under a generated name, which is referenced
     * in the returned predicate as a `$`-prefixed placeholder.
     *
     * @return the predicate string using the form `metadata.{key} INSIDE $<paramName>` where `<paramName>` is the generated parameter name
     */
    private String map(IsIn filter) {
        String paramName = createParam(filter.comparisonValues());
        return String.format("metadata.%s INSIDE $%s", filter.key(), paramName);
    }

    /**
     * Converts an IsNotIn filter into a SurrealDB predicate that checks the field is not inside a parameterized collection.
     *
     * @param filter the IsNotIn filter containing the metadata key and comparison values
     * @return a predicate string like <code>metadata.&lt;key&gt; NOTINSIDE $&lt;paramName&gt;</code> with a generated parameter name
     */
    private String map(IsNotIn filter) {
        String paramName = createParam(filter.comparisonValues());
        return String.format("metadata.%s NOTINSIDE $%s", filter.key(), paramName);
    }

    /**
     * Produces a SurrealDB `AND` expression combining the mapped left and right child filters.
     *
     * @return the parenthesized conjunction of the mapped left and right filters, formatted as "(left) AND (right)".
     */
    private String map(And filter) {
        return String.format("(%s) AND (%s)", map(filter.left()), map(filter.right()));
    }

    /**
     * Map an Or filter to a parenthesized SurrealDB `OR` expression.
     *
     * @param filter the Or filter whose left and right operands will be mapped and combined
     * @return a SurrealDB predicate string in the form "(left) OR (right)"
     */
    private String map(Or filter) {
        return String.format("(%s) OR (%s)", map(filter.left()), map(filter.right()));
    }

    /**
     * Negates the mapped predicate of the given Not filter.
     *
     * @param filter the Not filter containing the expression to negate
     * @return the SurrealDB predicate string representing the negated expression wrapped in `!(...)`
     */
    private String map(Not filter) {
        return String.format("!(%s)", map(filter.expression()));
    }

    /**
     * Generates a unique parameter name, stores the provided value in the parameters map under that name, and returns the name.
     *
     * @param value the value to bind as a query parameter
     * @return the generated parameter name (for example `filter_param_0`)
     */
    private String createParam(Object value) {
        String name = "filter_param_" + paramCounter.getAndIncrement();
        parameters.put(name, value);
        return name;
    }
}