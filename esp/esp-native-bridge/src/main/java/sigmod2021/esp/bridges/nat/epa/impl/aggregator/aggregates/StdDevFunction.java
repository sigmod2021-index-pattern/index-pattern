package sigmod2021.esp.bridges.nat.epa.impl.aggregator.aggregates;

import sigmod2021.util.Pair;

/**
 * Partial aggregate for standard-deviance computations.
 */
public class StdDevFunction implements AggregateFunction<Pair<Double, Double>, Double, Number> {

    private static final long serialVersionUID = 1L;

    /**
     * @{inheritDoc
     */
    @Override
    public Pair<Double, Double> fInit() {
        return new Pair<>(0.0, 0.0);
    }

    /**
     * @{inheritDoc
     */
    @Override
    public Pair<Double, Double> fInit(Number initialValue) {
        double v = initialValue.doubleValue();
        return new Pair<>(v, v * v);
    }

    /**
     * @{inheritDoc
     */
    @Override
    public Pair<Double, Double> fMerge(Pair<Double, Double> left, Pair<Double, Double> right) {
        return new Pair<>(left._1 + right._1, left._2 + right._2);
    }

    /**
     * @{inheritDoc
     */
    @Override
    public Double fEval(Pair<Double, Double> value, long counter) {
        if (counter < 2)
            return 0.0;
        return Math.sqrt(Math.max(0,
                (1.0 / (counter - 1)) * (value._2 - ((1.0 / counter) * (value._1 * value._1)))
        ));
    }
}
