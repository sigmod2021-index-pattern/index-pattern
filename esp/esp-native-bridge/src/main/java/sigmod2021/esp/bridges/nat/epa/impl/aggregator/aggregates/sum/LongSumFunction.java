package sigmod2021.esp.bridges.nat.epa.impl.aggregator.aggregates.sum;

import sigmod2021.esp.bridges.nat.epa.impl.aggregator.aggregates.AggregateFunction;

/**
 * Partial aggregate for sum computations on long-values.
 */
public class LongSumFunction implements AggregateFunction<Long, Long, Number> {

    private static final long serialVersionUID = 1L;

    /**
     * @{inheritDoc
     */
    @Override
    public Long fInit() {
        return 0L;
    }

    /**
     * @{inheritDoc
     */
    @Override
    public Long fMerge(Long left, Long right) {
        return left + right;
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
    public Long fInit(Number initialValue) {
        return initialValue.longValue();
    }

}
