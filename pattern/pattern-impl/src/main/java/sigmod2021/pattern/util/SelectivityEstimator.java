package sigmod2021.pattern.util;

import sigmod2021.esp.api.expression.BooleanExpression;

/**
 * Estimator interface for boolean expression selectivity.
 */
public interface SelectivityEstimator {

    /**
     * Estimates the selectivity of the given boolean expression.
     *
     * @param exp the boolean expression
     * @return the selectivity of the given boolean expression
     */
    double estimateSelectivity(BooleanExpression exp);
}
