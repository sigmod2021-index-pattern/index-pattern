package sigmod2021.esp.api.expression.predicate;

import sigmod2021.esp.api.expression.ArithmeticExpression;
import sigmod2021.esp.api.expression.Predicate;
import sigmod2021.esp.api.expression.base.AbstractExpression;

/**
 * Checks if the left input is greater than or equal to the right one
 */
public class GreaterEq extends AbstractExpression<ArithmeticExpression> implements Predicate {

    /**
     * @param left  the first input
     * @param right the second input
     */
    public GreaterEq(ArithmeticExpression left, ArithmeticExpression right) {
        super(left, right);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "(" + getInput(0) + ") >= (" + getInput(1) + ")";
    }
}
