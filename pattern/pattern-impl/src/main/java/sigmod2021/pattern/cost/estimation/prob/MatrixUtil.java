package sigmod2021.pattern.cost.estimation.prob;

import org.apache.commons.math3.linear.*;
import org.apache.commons.math3.util.FastMath;

public class MatrixUtil {

    // constants for pade approximation
    static final double c0 = 1.0;
    static final double c1 = 0.5;
    static final double c2 = 0.12;
    static final double c3 = 0.01833333333333333;
    static final double c4 = 0.0019927536231884053;
    static final double c5 = 1.630434782608695E-4;
    static final double c6 = 1.0351966873706E-5;
    static final double c7 = 5.175983436853E-7;
    static final double c8 = 2.0431513566525E-8;
    static final double c9 = 6.306022705717593E-10;
    static final double c10 = 1.4837700484041396E-11;
    static final double c11 = 2.5291534915979653E-13;
    static final double c12 = 2.8101705462199615E-15;
    static final double c13 = 1.5440497506703084E-17;


    // Faithfully adapted from jblas (http://mikiobraun.github.io/jblas/)
    public static RealMatrix matrixExponential(RealMatrix A) {
        if (!A.isSquare())
            throw new NonSquareMatrixException(A.getRowDimension(), A.getColumnDimension());

        int j = FastMath.max(0, 1 + (int) FastMath.floor(FastMath.log(2, getMaxElement(A))));
        RealMatrix As = A.scalarMultiply(1.0 / FastMath.pow(2, j)); // scaled version of A
        int n = A.getRowDimension();

        // calculate D and N using special Horner techniques
        RealMatrix As_2 = As.multiply(As);
        RealMatrix As_4 = As_2.multiply(As_2);
        RealMatrix As_6 = As_4.multiply(As_2);


        // U = c0*I + c2*A^2 + c4*A^4 + (c6*I + c8*A^2 + c10*A^4 + c12*A^6)*A^6
        RealMatrix U = LinearArrayRealMatrix.createDiagonal(n, c0)
                .add(As_2.scalarMultiply(c2))
                .add(As_4.scalarMultiply(c4))
                .add(LinearArrayRealMatrix.createDiagonal(n, c6)
                        .add(As_2.scalarMultiply(c8))
                        .add(As_4.scalarMultiply(c10))
                        .add(As_6.scalarMultiply(c12))
                        .multiply(As_6));

        // V = c1*I + c3*A^2 + c5*A^4 + (c7*I + c9*A^2 + c11*A^4 + c13*A^6)*A^6
        RealMatrix V = LinearArrayRealMatrix.createDiagonal(n, c1)
                .add(As_2.scalarMultiply(c3))
                .add(As_4.scalarMultiply(c5))
                .add(LinearArrayRealMatrix.createDiagonal(n, c7)
                        .add(As_2.scalarMultiply(c9))
                        .add(As_4.scalarMultiply(c11))
                        .add(As_6.scalarMultiply(c13))
                        .multiply(As_6));

        RealMatrix AV = As.multiply(V);
        RealMatrix N = U.add(AV);
        RealMatrix D = U.subtract(AV);

        // solve DF = N for F
        DecompositionSolver solver = new LUDecomposition(D).getSolver();
        RealMatrix F = solver.solve(N);

        // now square j times
        for (int k = 0; k < j; k++) {
            F = F.power(2);
        }
        return F;
    }

    private static double getMaxElement(RealMatrix rm) {
        RealMatrixPreservingVisitor v = new RealMatrixPreservingVisitor() {

            double max = Double.NEGATIVE_INFINITY;

            public void visit(int row, int column, double value) {
                max = FastMath.max(max, value);
            }

            public void start(int rows, int columns, int startRow, int endRow, int startColumn, int endColumn) {
            }

            public double end() {
                return max;
            }
        };
        rm.walkInOptimizedOrder(v);
        return v.end();
    }

}
