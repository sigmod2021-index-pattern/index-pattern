package sigmod2021.esp.bridges.nat.epa.impl.aggregator.aggregates.first;

import sigmod2021.esp.bridges.nat.epa.impl.aggregator.aggregates.AggregateFunction;

public class FirstFunction<T> implements AggregateFunction<T, T, T> {

    /** The serialVersionUID */
    private static final long serialVersionUID = 1L;

    /**
     * @{inheritDoc}
     */
    @Override
    public T fInit() {
        return null;
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public T fInit(T initialValue) {
        return initialValue;
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public T fMerge(T left, T right) {
        if (left == null)
            return right;
        return left;
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public T fEval(T value, long counter) {
        return value;
    }

}
