package sigmod2021.esp.bridges.nat.epa.impl.aggregator.aggregates.min;

import sigmod2021.esp.bridges.nat.epa.impl.aggregator.aggregates.AggregateFunction;

/**
 * Partial aggregate for minimum computations on float-values.
 */
public class FloatMinFunction implements AggregateFunction<Float, Float, Float> {

    private static final long serialVersionUID = 1L;

    /**
     * @{inheritDoc
     */
    @Override
    public Float fInit() {
        return Float.POSITIVE_INFINITY;
    }

    /**
     * @{inheritDoc
     */
    @Override
    public Float fMerge(Float left, Float right) {
        return right < left ? right : left;
    }

    /**
     * @{inheritDoc
     */
    @Override
    public Float fEval(Float value, long counter) {
        return value;
    }

    /**
     * @{inheritDoc
     */
    @Override
    public Float fInit(Float initialValue) {
        return initialValue;
    }

}
