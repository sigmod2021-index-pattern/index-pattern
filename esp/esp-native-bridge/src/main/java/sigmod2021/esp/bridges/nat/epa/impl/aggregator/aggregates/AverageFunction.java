package sigmod2021.esp.bridges.nat.epa.impl.aggregator.aggregates;

/**
 * Partial aggregate for average computations.
 */
public class AverageFunction implements AggregateFunction<Double, Double, Number> {

    private static final long serialVersionUID = 1L;

    /**
     * @{inheritDoc
     */
    @Override
    public Double fInit() {
        return 0.0;
    }

    /**
     * @{inheritDoc
     */
    @Override
    public Double fInit(Number initialValue) {
        return initialValue.doubleValue();
    }

    /**
     * @{inheritDoc
     */
    @Override
    public Double fMerge(Double left, Double right) {
        return left + right;
    }

    /**
     * @{inheritDoc
     */
    @Override
    public Double fEval(Double value, long counter) {
        return value / counter;
    }
}
