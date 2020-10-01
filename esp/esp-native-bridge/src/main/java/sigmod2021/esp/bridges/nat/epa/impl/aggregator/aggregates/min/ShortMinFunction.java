package sigmod2021.esp.bridges.nat.epa.impl.aggregator.aggregates.min;

import sigmod2021.esp.bridges.nat.epa.impl.aggregator.aggregates.AggregateFunction;

/**
 * Partial aggregate for minimum computations on short-values.
 */
public class ShortMinFunction implements AggregateFunction<Short, Short, Short> {

    private static final long serialVersionUID = 1L;

    /**
     * @{inheritDoc
     */
    @Override
    public Short fInit() {
        return Short.MAX_VALUE;
    }

    /**
     * @{inheritDoc
     */
    @Override
    public Short fMerge(Short left, Short right) {
        return right < left ? right : left;
    }

    /**
     * @{inheritDoc
     */
    @Override
    public Short fEval(Short value, long counter) {
        return value;
    }

    /**
     * @{inheritDoc
     */
    @Override
    public Short fInit(Short initialValue) {
        return initialValue;
    }

}
