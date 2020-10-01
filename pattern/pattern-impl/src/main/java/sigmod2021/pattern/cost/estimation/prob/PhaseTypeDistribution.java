package sigmod2021.pattern.cost.estimation.prob;

import org.apache.commons.math3.distribution.AbstractRealDistribution;
import org.apache.commons.math3.linear.*;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.commons.math3.random.Well19937c;
import org.apache.commons.math3.util.FastMath;

public class PhaseTypeDistribution extends AbstractRealDistribution {

    private static final long serialVersionUID = 1L;

    private final RealMatrix S;

    private final RealMatrix alpha;

    private RealMatrix S0;

    private Double mean;

    private Double variance;


    public PhaseTypeDistribution(RealMatrix S, RealMatrix alpha) {
        this(new Well19937c(), S, alpha);
    }

    public PhaseTypeDistribution(RandomGenerator rng, RealMatrix S, RealMatrix alpha) {
        super(rng);
        if (!S.isSquare())
            throw new NonSquareMatrixException(S.getRowDimension(), S.getColumnDimension());
        if (alpha.getRowDimension() != 1 || alpha.getColumnDimension() != S.getRowDimension())
            throw new MatrixDimensionMismatchException(alpha.getRowDimension(), alpha.getColumnDimension(), 1, S.getRowDimension());

        this.S = S;
        this.alpha = alpha;
    }

    public double density(double x) {
        if (S0 == null) {
            LinearArrayRealMatrix identity = new LinearArrayRealMatrix(S.getColumnDimension(), 1);
            identity.walkInOptimizedOrder(new RealMatrixChangingVisitor() {
                @Override
                public double visit(int row, int column, double value) {
                    return 1.0;
                }

                @Override
                public void start(int rows, int columns, int startRow, int endRow, int startColumn, int endColumn) {
                }

                @Override
                public double end() {
                    return 0;
                }
            });
            S0 = S.scalarMultiply(-1).multiply(identity);
        }
        return alpha.multiply(MatrixUtil.matrixExponential(S.scalarMultiply(x))).multiply(S0).getEntry(0, 0);
    }

    public double cumulativeProbability(double x) {
        return 1.0 - sum(alpha.multiply(MatrixUtil.matrixExponential(S.scalarMultiply(x))));
    }

    private double sum(RealMatrix m) {
        return m.walkInOptimizedOrder(new RealMatrixPreservingVisitor() {

            double sum = 0;

            @Override
            public void visit(int row, int column, double value) {
                sum += value;
            }

            @Override
            public void start(int rows, int columns, int startRow, int endRow, int startColumn, int endColumn) {
            }

            @Override
            public double end() {
                return sum;
            }
        });
    }

    public double getNumericalMean() {
        if (mean == null) {
            mean = -sum(alpha.multiply(MatrixUtils.inverse(S)));
        }
        return mean;
    }

    public double getNumericalVariance() {
        if (variance == null) {
            RealMatrix SINV = MatrixUtils.inverse(S);
            variance = 2.0 * sum(alpha.multiply(SINV.power(2))) - FastMath.pow(sum(alpha.multiply(SINV)), 2);
        }
        return variance;
    }

    public double getSupportLowerBound() {
        return 0;
    }

    public double getSupportUpperBound() {
        return Double.POSITIVE_INFINITY;
    }

    public boolean isSupportLowerBoundInclusive() {
        return true;
    }

    public boolean isSupportUpperBoundInclusive() {
        return false;
    }

    public boolean isSupportConnected() {
        return true;
    }
}
