package sigmod2021.pattern.cost.estimation.prob;

import org.apache.commons.math3.exception.DimensionMismatchException;
import org.apache.commons.math3.exception.NotStrictlyPositiveException;
import org.apache.commons.math3.exception.OutOfRangeException;
import org.apache.commons.math3.linear.*;

public class LinearArrayRealMatrix extends AbstractRealMatrix {

    private final double[] elements;

    private final int m, n;

    public LinearArrayRealMatrix(int m, int n) {
        super(m, n);
        elements = new double[m * n];
        this.m = m;
        this.n = n;
    }

    public static LinearArrayRealMatrix createIdentity(int n) {
        return createDiagonal(n, 1.0);
    }

    public static LinearArrayRealMatrix createDiagonal(int n, double v) {
        LinearArrayRealMatrix result = new LinearArrayRealMatrix(n, n);
        for (int i = 0; i < n; i++) {
            result.elements[result.index(i, i)] = v;
        }
        return result;
    }

    private final int index(int row, int col) {
        return row * n + col;
    }

    @Override
    public int getRowDimension() {
        return m;
    }

    @Override
    public int getColumnDimension() {
        return n;
    }

    @Override
    public double[][] getData() {
        double[][] result = new double[m][n];
        for (int i = 0; i < m; i++) {
            System.arraycopy(elements, n * i, result[i], 0, n);
        }
        return result;
    }

    @Override
    public RealMatrix add(RealMatrix a) throws MatrixDimensionMismatchException {
        MatrixUtils.checkAdditionCompatible(this, a);
        LinearArrayRealMatrix result = new LinearArrayRealMatrix(m, n);

        if (a instanceof LinearArrayRealMatrix) {
            LinearArrayRealMatrix larm = (LinearArrayRealMatrix) a;
            for (int i = 0; i < elements.length; i++) {
                result.elements[i] = elements[i] + larm.elements[i];
            }
        } else {
            for (int i = 0; i < elements.length; i++) {
                result.elements[i] = elements[i] + a.getEntry(i / n, i % n);
            }
        }
        return result;
    }

    @Override
    public RealMatrix subtract(RealMatrix a) throws MatrixDimensionMismatchException {
        MatrixUtils.checkSubtractionCompatible(this, a);
        LinearArrayRealMatrix result = new LinearArrayRealMatrix(m, n);

        if (a instanceof LinearArrayRealMatrix) {
            LinearArrayRealMatrix larm = (LinearArrayRealMatrix) a;
            for (int i = 0; i < elements.length; i++) {
                result.elements[i] = elements[i] - larm.elements[i];
            }
        } else {
            for (int i = 0; i < elements.length; i++) {
                result.elements[i] = elements[i] - a.getEntry(i / n, i % n);
            }
        }
        return result;
    }

    @Override
    public LinearArrayRealMatrix scalarAdd(double d) {
        LinearArrayRealMatrix result = new LinearArrayRealMatrix(m, n);
        for (int i = 0; i < elements.length; i++)
            result.elements[i] = elements[i] + d;
        return result;
    }

    @Override
    public LinearArrayRealMatrix scalarMultiply(double d) {
        LinearArrayRealMatrix result = new LinearArrayRealMatrix(m, n);
        for (int i = 0; i < elements.length; i++)
            result.elements[i] = elements[i] * d;
        return result;
    }

    @Override
    public LinearArrayRealMatrix transpose() {
        LinearArrayRealMatrix result = new LinearArrayRealMatrix(n, m);
        for (int r = 0; r < n; r++) {
            for (int c = 0; c < m; c++) {
                result.elements[result.index(r, c)] = elements[index(c, r)];
            }
        }
        return result;
    }

    @Override
    public LinearArrayRealMatrix multiply(RealMatrix a) throws DimensionMismatchException {
        MatrixUtils.checkMultiplicationCompatible(this, a);

        LinearArrayRealMatrix result = new LinearArrayRealMatrix(m, a.getColumnDimension());

        if (a instanceof LinearArrayRealMatrix) {
//			LinearArrayRealMatrix other = ((LinearArrayRealMatrix) a).transpose();
            LinearArrayRealMatrix other = (LinearArrayRealMatrix) a;
            for (int row = 0; row < result.m; row++) {
                for (int col = 0; col < result.n; col++) {
                    double s = 0.0;
                    for (int x = 0; x < n; x++) {
//						s += elements[row*n+x]* other.elements[col*n+x];
                        s += elements[index(row, x)] * other.elements[other.index(x, col)];
                    }
                    result.elements[row * result.n + col] = s;
                }
            }
        } else {
            double[][] oData = a.getData();
            for (int row = 0; row < result.m; row++) {
                for (int col = 0; col < result.n; col++) {
                    double s = 0.0;
                    for (int x = 0; x < n; x++) {
                        s += elements[index(row, x)] * oData[x][col];
                    }
                    result.elements[result.index(row, col)] = s;
                }
            }
        }
        return result;
    }

    @Override
    public RealMatrix getRowMatrix(int row) throws OutOfRangeException {
        MatrixUtils.checkRowIndex(this, row);
        LinearArrayRealMatrix result = new LinearArrayRealMatrix(1, n);
        System.arraycopy(elements, row * n, result.elements, 0, n);
        return result;
    }

    @Override
    public void setRowMatrix(int row, RealMatrix matrix) throws OutOfRangeException, MatrixDimensionMismatchException {
        MatrixUtils.checkRowIndex(this, row);
        if (matrix.getColumnDimension() != n)
            throw new MatrixDimensionMismatchException(matrix.getRowDimension(), matrix.getColumnDimension(), 1, n);
        final int begin = row * n;
        for (int i = 0; i < n; i++) {
            elements[begin + i] = matrix.getEntry(0, i);
        }
    }

    @Override
    public double[] getRow(int row) throws OutOfRangeException {
        MatrixUtils.checkRowIndex(this, row);
        double[] result = new double[n];
        System.arraycopy(elements, row * n, result, 0, n);
        return result;
    }

    @Override
    public void setRow(int row, double[] array) throws OutOfRangeException, MatrixDimensionMismatchException {
        MatrixUtils.checkRowIndex(this, row);
        if (array.length != n)
            throw new MatrixDimensionMismatchException(1, array.length, 1, n);
        System.arraycopy(array, 0, elements, row * n, n);
    }

    @Override
    public void addToEntry(int row, int column, double increment) throws OutOfRangeException {
        MatrixUtils.checkRowIndex(this, row);
        MatrixUtils.checkColumnIndex(this, column);
        elements[index(row, column)] += increment;
    }

    @Override
    public void multiplyEntry(int row, int column, double factor) throws OutOfRangeException {
        MatrixUtils.checkRowIndex(this, row);
        MatrixUtils.checkColumnIndex(this, column);
        elements[index(row, column)] *= factor;
    }

    @Override
    public double walkInRowOrder(RealMatrixChangingVisitor visitor) {
        visitor.start(m, n, 0, m - 1, 0, n - 1);
        for (int i = 0; i < elements.length; i++)
            elements[i] = visitor.visit(i / n, i % n, elements[i]);
        return visitor.end();
    }

    @Override
    public double walkInRowOrder(RealMatrixPreservingVisitor visitor) {
        visitor.start(m, n, 0, m - 1, 0, n - 1);
        for (int i = 0; i < elements.length; i++)
            visitor.visit(i / n, i % n, elements[i]);
        return visitor.end();
    }

    @Override
    public LinearArrayRealMatrix createMatrix(int rowDimension, int columnDimension) throws NotStrictlyPositiveException {
        return new LinearArrayRealMatrix(m, n);
    }

    @Override
    public LinearArrayRealMatrix copy() {
        LinearArrayRealMatrix result = new LinearArrayRealMatrix(m, n);
        System.arraycopy(elements, 0, result.elements, 0, elements.length);
        return null;
    }

    @Override
    public double getEntry(int row, int column) throws OutOfRangeException {
        MatrixUtils.checkMatrixIndex(this, row, column);
        return elements[index(row, column)];
    }

    @Override
    public void setEntry(int row, int column, double value) throws OutOfRangeException {
        MatrixUtils.checkMatrixIndex(this, row, column);
        elements[index(row, column)] = value;
    }
}
