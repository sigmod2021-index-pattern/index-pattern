package sigmod2021.esp.api.expression.predicate.spatial;

import sigmod2021.esp.api.expression.ArithmeticExpression;
import sigmod2021.esp.api.expression.Predicate;
import sigmod2021.esp.api.expression.base.AbstractExpression;

/**
 * Checks if the left input overlaps the right one
 */
public class Overlaps extends AbstractExpression<ArithmeticExpression> implements Predicate {


    /**
     * @param left  the first input
     * @param right the second input
     */
    public Overlaps(ArithmeticExpression left, ArithmeticExpression right) {
        super(left, right);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "(" + getInput(0) + ") overlaps (" + getInput(1) + ")";
    }


}
