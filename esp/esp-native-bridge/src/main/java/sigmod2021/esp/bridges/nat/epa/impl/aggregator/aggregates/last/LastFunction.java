package sigmod2021.esp.bridges.nat.epa.impl.aggregator.aggregates.last;

import sigmod2021.esp.bridges.nat.epa.impl.aggregator.aggregates.AggregateFunction;


/**
 *
 */
public class LastFunction<T> implements AggregateFunction<T, T, T> {

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
        return right;
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public T fEval(T value, long counter) {
        return value;
    }

}
