package sigmod2021.esp.bridges.nat.epa.impl.aggregator.aggregates.min;

import sigmod2021.esp.bridges.nat.epa.impl.aggregator.aggregates.AggregateFunction;

/**
 * Partial aggregate for minimum computations on int-values.
 */
public class IntMinFunction implements AggregateFunction<Integer, Integer, Integer> {

    private static final long serialVersionUID = 1L;

    /**
     * @{inheritDoc
     */
    @Override
    public Integer fInit() {
        return Integer.MAX_VALUE;
    }

    /**
     * @{inheritDoc
     */
    @Override
    public Integer fMerge(Integer left, Integer right) {
        return right < left ? right : left;
    }

    /**
     * @{inheritDoc
     */
    @Override
    public Integer fEval(Integer value, long counter) {
        return value;
    }

    /**
     * @{inheritDoc
     */
    @Override
    public Integer fInit(Integer initialValue) {
        return initialValue;
    }

}
