package sigmod2021.esp.bridges.nat.epa.impl.aggregator.aggregates.min;

import sigmod2021.esp.bridges.nat.epa.impl.aggregator.aggregates.AggregateFunction;

/**
 * Partial aggregate for minimum computations on byte-values.
 */
public class ByteMinFunction implements AggregateFunction<Byte, Byte, Byte> {

    private static final long serialVersionUID = 1L;

    /**
     * @{inheritDoc
     */
    @Override
    public Byte fInit() {
        return Byte.MAX_VALUE;
    }

    /**
     * @{inheritDoc
     */
    @Override
    public Byte fMerge(Byte left, Byte right) {
        return right < left ? right : left;
    }

    /**
     * @{inheritDoc
     */
    @Override
    public Byte fEval(Byte value, long counter) {
        return value;
    }

    /**
     * @{inheritDoc
     */
    @Override
    public Byte fInit(Byte initialValue) {
        return initialValue;
    }

}
