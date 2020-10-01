package sigmod2021.esp.api.expression.predicate;

import sigmod2021.esp.api.expression.ArithmeticExpression;
import sigmod2021.esp.api.expression.Predicate;
import sigmod2021.esp.api.expression.base.AbstractExpression;

/**
 * Compares two inputs for equality.
 */
public class Equal extends AbstractExpression<ArithmeticExpression> implements Predicate {

    /**
     * @param left  the first input
     * @param right the second input
     */
    public Equal(ArithmeticExpression left, ArithmeticExpression right) {
        super(left, right);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "(" + getInput(0) + ") == (" + getInput(1) + ")";
    }
}
