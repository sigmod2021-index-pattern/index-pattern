package sigmod2021.esp.bridges.nat.epa.impl.aggregator.aggregates.min;

import sigmod2021.esp.bridges.nat.epa.impl.aggregator.aggregates.AggregateFunction;

/**
 * Partial aggregate for minimum computations on long-values.
 */
public class LongMinFunction implements AggregateFunction<Long, Long, Long> {

    private static final long serialVersionUID = 1L;

    /**
     * @{inheritDoc
     */
    @Override
    public Long fInit() {
        return Long.MAX_VALUE;
    }

    /**
     * @{inheritDoc
     */
    @Override
    public Long fMerge(Long left, Long right) {
        return right < left ? right : left;
    }

    /**
     * @{inheritDoc
     */
    @Override
    public Long fEval(Long value, long counter) {
        return value;
    }

    /**
     * @{inheritDoc
     */
    @Override
    public Long fInit(Long initialValue) {
        return initialValue;
    }

}
