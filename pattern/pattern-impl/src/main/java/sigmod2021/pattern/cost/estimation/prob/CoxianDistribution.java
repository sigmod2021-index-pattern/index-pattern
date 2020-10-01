package sigmod2021.pattern.cost.estimation.prob;

import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.random.RandomGenerator;

public class CoxianDistribution extends PhaseTypeDistribution {

    private static final long serialVersionUID = 1L;

    public CoxianDistribution(double[] lambdas, double[] ps) {
        super(createS(lambdas, ps), createAlpha(lambdas.length));
    }

    public CoxianDistribution(RandomGenerator rng, double[] lambdas, double[] ps) {
        super(rng, createS(lambdas, ps), createAlpha(lambdas.length));
    }

    private static RealMatrix createS(double[] lambdas, double[] ps) {
        RealMatrix res = new LinearArrayRealMatrix(lambdas.length, lambdas.length);
//		RealMatrix res = MatrixUtils.createRealMatrix(lambdas.length, lambdas.length);
        for (int i = 0; i < lambdas.length - 1; i++) {
            res.setEntry(i, i, -lambdas[i]);
            res.setEntry(i, i + 1, ps[i] * lambdas[i]);
        }
        res.setEntry(lambdas.length - 1, lambdas.length - 1, -lambdas[lambdas.length - 1]);
        return res;
    }

    private static RealMatrix createAlpha(int length) {
        RealMatrix res = new LinearArrayRealMatrix(1, length);
//		RealMatrix res = MatrixUtils.createRealMatrix(1, length);
        res.setEntry(0, 0, 1.0);
        return res;
    }

}
