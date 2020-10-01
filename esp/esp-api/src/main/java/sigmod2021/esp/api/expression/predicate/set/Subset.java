package sigmod2021.esp.api.expression.predicate.set;

import sigmod2021.esp.api.expression.ArithmeticExpression;
import sigmod2021.esp.api.expression.Predicate;
import sigmod2021.esp.api.expression.base.AbstractExpression;

/**
 * Check if the first input is subset of the second input
 *
 */
public class Subset extends AbstractExpression<ArithmeticExpression> implements Predicate {


    /**
     * @param left the first input
     * @param right the second input
     */
    public Subset(ArithmeticExpression left, ArithmeticExpression right) {
        super(left, right);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "(" + getInput(0) + ") is subset of (" + getInput(1) + ")";
    }


}
