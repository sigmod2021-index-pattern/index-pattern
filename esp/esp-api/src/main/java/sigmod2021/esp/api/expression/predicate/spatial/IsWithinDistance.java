package sigmod2021.esp.api.expression.predicate.spatial;

import sigmod2021.esp.api.expression.ArithmeticExpression;
import sigmod2021.esp.api.expression.Predicate;
import sigmod2021.esp.api.expression.base.AbstractExpression;

/**
 * Checks if the left input is within distance from the right one
 */
public class IsWithinDistance extends AbstractExpression<ArithmeticExpression> implements Predicate {

    private final double distance;

    /**
     * @param distance the distance
     * @param left     the first input
     * @param right    the second input
     */
    public IsWithinDistance(double distance, ArithmeticExpression left, ArithmeticExpression right) {
        super(left, right);
        this.distance = distance;
    }

    public double getDistance() {
        return distance;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "(" + getInput(0) + ") is within " + distance + " distance from (" + getInput(1) + ")";
    }


}
