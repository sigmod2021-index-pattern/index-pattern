package sigmod2021.esp.bridges.nat.epa.impl.aggregator.aggregates.max;

import sigmod2021.esp.bridges.nat.epa.impl.aggregator.aggregates.AggregateFunction;

/**
 * Partial aggregate for maximum computations on float-values.
 */
public class FloatMaxFunction implements AggregateFunction<Float, Float, Float> {

    private static final long serialVersionUID = 1L;

    /**
     * @{inheritDoc
     */
    @Override
    public Float fInit() {
        return Float.NEGATIVE_INFINITY;
    }

    /**
     * @{inheritDoc
     */
    @Override
    public Float fMerge(Float left, Float right) {
        return right > left ? right : left;
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
