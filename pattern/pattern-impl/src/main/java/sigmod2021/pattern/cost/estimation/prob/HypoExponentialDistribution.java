package sigmod2021.pattern.cost.estimation.prob;

import org.apache.commons.math3.random.RandomGenerator;

import java.util.Arrays;
import java.util.List;

public class HypoExponentialDistribution extends CoxianDistribution {

    private static final long serialVersionUID = 1L;

    public HypoExponentialDistribution(List<Double> lambdas) {
        this(toArray(lambdas));
    }

    public HypoExponentialDistribution(RandomGenerator rng, List<Double> lambdas) {
        this(rng, toArray(lambdas));
    }

    public HypoExponentialDistribution(double... lambdas) {
        super(lambdas, createProps(lambdas.length));
    }

    public HypoExponentialDistribution(RandomGenerator rng, double... lambdas) {
        super(rng, lambdas, createProps(lambdas.length));
    }

    private static double[] createProps(int length) {
        double[] res = new double[length];
        Arrays.fill(res, 1.0);
        return res;
    }

    private static double[] toArray(List<Double> l) {
        double[] result = new double[l.size()];
        for (int i = 0; i < result.length; i++)
            result[i] = l.get(i).doubleValue();
        return result;
    }
}
