package sigmod2021.esp.bridges.nat.epa.impl.aggregator.aggregates.max;

import sigmod2021.esp.bridges.nat.epa.impl.aggregator.aggregates.AggregateFunction;

/**
 * Partial aggregate for maximum computations on double-values.
 */
public class DoubleMaxFunction implements AggregateFunction<Double, Double, Double> {

    private static final long serialVersionUID = 1L;

    /**
     * @{inheritDoc
     */
    @Override
    public Double fInit() {
        return Double.NEGATIVE_INFINITY;
    }

    /**
     * @{inheritDoc
     */
    @Override
    public Double fMerge(Double left, Double right) {
        return right > left ? right : left;
    }

    /**
     * @{inheritDoc
     */
    @Override
    public Double fEval(Double value, long counter) {
        return value;
    }

    /**
     * @{inheritDoc
     */
    @Override
    public Double fInit(Double initialValue) {
        return initialValue;
    }

}
